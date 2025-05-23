import pandas as pd
import re
from pathlib import Path
from typing import Tuple, Optional
import logging
import chardet
import threading

from .data_combiner import DataCombiner


class FileProcessor:
    def __init__(self, inventory_manager):
        self.inventory_manager = inventory_manager
        self.logger = logging.getLogger(__name__)
        
        # Mapeamento de colunas do Excel
        self.excel_column_mapping = {
            'GTIN': ['gtin', 'ean', 'código de barras', 'barcode'],
            'Codigo': ['codigo', 'código', 'id', 'produto', 'sku'],
            'Descricao': ['descricao', 'descrição', 'nome', 'produto'],
            'Preco': ['preco', 'preço', 'valor', 'preço de venda'],
            'Estoque': ['estoque', 'quantidade', 'qtd', 'saldo'],
            'Custo': ['custo', 'custo unitário', 'preço de custo'],
            'Secao': ['secao', 'seção', 'departamento', 'categoria'],
            'Endereco': ['endereco', 'endereço', 'local', 'posição'],
            'Operador': ['operador', 'funcionário', 'responsável']
        }

    @staticmethod
    def detect_encoding(file_path: str) -> Optional[str]:
        """Detecta a codificação de um arquivo com fallback inteligente."""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)
                result = chardet.detect(raw_data)
                if result['confidence'] < 0.7 or not result['encoding']:
                    return 'utf-8'
                return result['encoding']
        except Exception as e:
            logging.warning(f"Falha ao detectar encoding: {e}")
            return 'utf-8'

    def _remove_leading_zeros(self, value: str) -> str:
        """Remove zeros à esquerda de uma string numérica."""
        return value.lstrip('0') or '0'

    def _add_flag_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Faz o join do DataFrame processado com o arquivo `prod_flag.parquet`.
        Adiciona a coluna `Flag` com base no arquivo `prod_flag.parquet`.
        """
        try:
            # Caminho para o arquivo prod_flag.parquet
            flag_file = Path(__file__).parent.parent / 'core' / 'prod_flag.parquet'

            if not flag_file.exists():
                self.logger.warning("Arquivo 'prod_flag.parquet' não encontrado.")
                df['Flag'] = ''  # Adiciona coluna vazia se arquivo não existir
                return df

            # Carrega o arquivo prod_flag.parquet
            flag_df = pd.read_parquet(flag_file)

            # Verifica colunas obrigatórias
            required_columns = {'produto_key', 'flag'}
            if not required_columns.issubset(flag_df.columns):
                self.logger.error(f"Colunas obrigatórias {required_columns} ausentes no arquivo 'prod_flag.parquet'.")
                df['Flag'] = ''
                return df

            # Pré-processamento - Normalização das chaves
            def normalize_code(code: str) -> str:
                """Remove todos os não-dígitos e zeros à esquerda"""
                if pd.isna(code):
                    return ''
                return ''.join(filter(str.isdigit, str(code))).lstrip('0') or '0'

            # Aplica normalização
            df['Codigo_normalized'] = df['Codigo'].apply(normalize_code)
            flag_df['produto_key_normalized'] = flag_df['produto_key'].apply(normalize_code)

            # Faz o join apenas pelo código normalizado
            merged_df = pd.merge(
                df,
                flag_df[['produto_key_normalized', 'flag']],
                how='left',
                left_on=['Codigo_normalized'],
                right_on=['produto_key_normalized']
            )

            # Adiciona a coluna Flag e remove colunas temporárias
            df['Flag'] = merged_df['flag'].fillna('')
            df.drop(columns=['Codigo_normalized'], inplace=True, errors='ignore')

            # Estatísticas
            flagged_count = (df['Flag'] != '').sum()
            self.logger.info(f"Flags aplicadas em {flagged_count} de {len(df)} produtos ({(flagged_count/len(df))*100:.2f}%)")
            
            # Debug: mostra exemplos de matches
            if flagged_count > 0:
                sample_matches = df[df['Flag'] != ''].head(3)[['Codigo', 'Flag']]
                self.logger.debug(f"Exemplos de matches:\n{sample_matches.to_string()}")
            
            return df

        except Exception as e:
            self.logger.error(f"Erro ao adicionar flags: {e}", exc_info=True)
            df['Flag'] = ''
            return df

    def process_initial_txt(self, file_path: str) -> Tuple[bool, str]:
        """Processa o arquivo TXT inicial e salva como parquet"""
        try:
            data_path = Path(self.inventory_manager.get_active_inventory_data_path())
            if not data_path:
                return False, "Nenhum inventário ativo selecionado."

            # Garante que a pasta existe
            data_path.mkdir(parents=True, exist_ok=True)

            # Detecta a codificação do arquivo TXT
            encodings_to_try = [self.detect_encoding(file_path), 'utf-8', 'latin-1']
            lines = []

            for encoding in filter(None, set(encodings_to_try)):
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        lines = f.readlines()
                    break
                except UnicodeDecodeError:
                    continue

            if not lines:
                return False, "Não foi possível ler o arquivo."

            # Expressão regular para parsing
            pattern = re.compile(
                r'^(?P<gtin>\d{13})\s+'
                r'(?P<codigo>\d{9})\s+'
                r'(?P<descricao>.+?)\s+'
                r'(?P<preco>\d{8})\s+'
                r'(?P<estoque>\d{8})\s+'
                r'(?P<custo>\d{8})\s+'
                r'(?P<secao>\d{5})$'
            )

            data = []
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue

                match = pattern.match(line)
                if not match:
                    self.logger.warning(f"Linha {line_num} ignorada: formato inválido.")
                    continue

                try:
                    data.append({
                        'GTIN': self._remove_leading_zeros(match.group('gtin')),
                        'Codigo': self._remove_leading_zeros(match.group('codigo')),
                        'Descricao': match.group('descricao').strip(),
                        'Preco': int(match.group('preco')) / 100,
                        'Estoque': int(match.group('estoque')),
                        'Custo': int(match.group('custo')) / 100,
                        'Secao': self._remove_leading_zeros(match.group('secao')),
                    })
                except Exception as e:
                    self.logger.error(f"Erro ao processar linha {line_num}: {e}")

            if not data:
                return False, "Nenhum dado válido encontrado."

            df = pd.DataFrame(data)
            df = self._add_flag_data(df)

            columns_order = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque',
                            'Custo', 'Secao', 'Flag']
            df = df[columns_order]

            # Salva os dados processados
            output_path = data_path / "initial_data.parquet"
            df.to_parquet(output_path, index=False)
            
            # Dispara a combinação automática
            combiner = DataCombiner(data_path)
            combiner.combine_data()
            
            return True, f"Arquivo processado com sucesso. {len(data)} itens importados."
        except Exception as e:
            self.logger.error(f"Erro ao processar TXT: {e}", exc_info=True)
            return False, f"Erro crítico: {str(e)}"
    
    def _trigger_data_combination(self, data_path: str) -> bool:
            """Dispara a combinação de dados"""
            try:
                return DataCombiner(data_path).combine_data()
            except Exception as e:
                self.logger.error(f"Falha na combinação automática: {e}", exc_info=True)
                return False
                
    def _map_excel_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mapeia as colunas EXATAS do seu Excel para o formato interno"""
        mapped_df = pd.DataFrame()
        
        # Mapeamento ESPECÍFICO para seus arquivos
        column_mapping = {
            'GTIN': ['CÓD. BARRAS', 'COD. BARRAS', 'CODIGO BARRAS'],
            'Operador': ['OPERADOR'],
            'Endereco': ['ENDEREÇO', 'ENDERECO'],
            'Estoque': ['QNT. CONTADA', 'QUANTIDADE CONTADA', 'QNT CONTADA']
        }
        
        # Verifica cada coluna necessária
        for standard_col, possible_names in column_mapping.items():
            found = False
            for possible_name in possible_names:
                if possible_name in df.columns:
                    mapped_df[standard_col] = df[possible_name]
                    self.logger.debug(f"Mapeada coluna '{possible_name}' -> '{standard_col}'")
                    found = True
                    break
            
            if not found and standard_col in ['Endereco', 'Operador']:
                mapped_df[standard_col] = None  # Colunas opcionais
                self.logger.debug(f"Coluna opcional '{standard_col}' não encontrada")
            elif not found:
                self.logger.warning(f"Coluna obrigatória '{standard_col}' não encontrada. Procurado por: {possible_names}")
                return None
    
        return mapped_df

    def _clean_excel_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processa os dados do SEU formato específico"""
        try:
            # Converte GTIN (que veio de CÓD. BARRAS)
            if 'GTIN' in df.columns:
                df['GTIN'] = (
                    df['GTIN']
                    .astype(str)
                    .str.replace(r'\D', '', regex=True)  # Remove não-dígitos
                    .apply(self._remove_leading_zeros)
                )
            
            # Converte estoque para numérico
            if 'Estoque' in df.columns:
                df['Estoque'] = pd.to_numeric(df['Estoque'], errors='coerce').fillna(0)
            
            # Processa texto das colunas opcionais
            for col in ['Operador', 'Endereco']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
            
            # Agrupa por GTIN (CÓD. BARRAS)
            if 'GTIN' in df.columns:
                agg_rules = {'Estoque': 'sum'}
                for col in ['Operador', 'Endereco']:
                    if col in df.columns:
                        agg_rules[col] = lambda x: ' / '.join(sorted(set(x.astype(str))))
                
                return df.groupby('GTIN', as_index=False).agg(agg_rules)
            
            return df
        except Exception as e:
            self.logger.error(f"Erro na limpeza dos dados: {str(e)}", exc_info=True)
            raise

    def process_excel(self, file_path: str) -> Tuple[bool, str]:
        """Processa arquivo Excel de contagem"""
        try:
            data_path = Path(self.inventory_manager.get_active_inventory_data_path())
            if not data_path:
                return False, "Nenhum inventário ativo selecionado"

            data_path.mkdir(exist_ok=True)

            # Carrega o arquivo Excel
            df = pd.read_excel(file_path, sheet_name=0)
            df.dropna(how='all', inplace=True)
            
            if len(df) < 1:
                return False, "Planilha vazia ou sem dados válidos"

            # Renomeia colunas
            column_mapping = {
                'cód. barras': 'COD_BARRAS',
                'cod. barras': 'COD_BARRAS',
                'codigo barras': 'COD_BARRAS',
                'qnt. contada': 'QNT_CONTADA',
                'quantidade contada': 'QNT_CONTADA',
                'operador': 'OPERADOR',
                'endereço': 'ENDERECO',
                'endereco': 'ENDERECO'
            }
            
            df.columns = [column_mapping.get(col.lower().strip(), col) for col in df.columns]
            
            # Verifica colunas obrigatórias
            required_columns = {'COD_BARRAS', 'QNT_CONTADA'}
            if not required_columns.issubset(df.columns):
                missing = required_columns - set(df.columns)
                return False, f"Colunas obrigatórias faltando: {missing}"

            # Processa dados
            df['COD_BARRAS'] = (
                df['COD_BARRAS']
                .astype(str)
                .str.replace(r'\.0$', '', regex=True)
                .str.replace(r'\D', '', regex=True)
                .apply(self._remove_leading_zeros)
            )

            df['QNT_CONTADA'] = pd.to_numeric(df['QNT_CONTADA'], errors='coerce').fillna(0)

            text_cols = ['OPERADOR', 'ENDERECO']
            for col in text_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()

            agg_rules = {'QNT_CONTADA': 'sum'}
            for col in text_cols:
                if col in df.columns:
                    agg_rules[col] = lambda x: ' / '.join(sorted(set(x.astype(str))))
            
            grouped_df = df.groupby('COD_BARRAS', as_index=False).agg(agg_rules)

            output_path = data_path / "dados123.parquet"
            
            # Carrega dados existentes se houver
            if output_path.exists():
                existing_df = pd.read_parquet(output_path)
                # Concatena e reagrupa mantendo a soma das quantidades
                final_df = pd.concat([existing_df, grouped_df])
                final_df = final_df.groupby('COD_BARRAS', as_index=False).agg(agg_rules)
            else:
                final_df = grouped_df

            # Salva o arquivo atualizado
            final_df.to_parquet(output_path, index=False)
            
            # Dispara a combinação automática
            combiner = DataCombiner(data_path)
            combiner.combine_data()

            return True, (f"Dados processados com sucesso. "
                        f"Total de {len(final_df)} itens únicos. "
                        f"Quantidade total: {final_df['QNT_CONTADA'].sum():.0f}")

        except Exception as e:
            self.logger.error(f"Erro ao processar Excel: {e}", exc_info=True)
            return False, f"Erro ao processar Excel: {str(e)}"