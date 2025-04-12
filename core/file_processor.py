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

    def _trigger_data_combination(self, data_path: str) -> bool:
        """Dispara a combinação de dados com tratamento de erros."""
        try:
            combiner = DataCombiner(data_path)
            success = combiner.combine_data()

            if success and hasattr(self.inventory_manager, 'notify_ui'):
                self.inventory_manager.notify_ui("data_updated")

            return success
        except Exception as e:
            self.logger.error(f"Falha na combinação automática: {e}", exc_info=True)
            return False

    def process_initial_txt(self, file_path: str) -> Tuple[bool, str]:
        """Processa o arquivo TXT inicial."""
        try:
            # Obtém o caminho da pasta de dados do inventário ativo
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if not data_path:
                return False, "Nenhum inventário ativo selecionado."

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

            # Expressão regular para parsing das linhas no arquivo TXT
            pattern = re.compile(
                r'^(?P<gtin>\d{13})\s+'         # GTIN (13 dígitos)
                r'(?P<codigo>\d{9})\s+'         # Codigo (9 dígitos)
                r'(?P<descricao>.+?)\s+'        # Descricao (texto variável)
                r'(?P<preco>\d{8})\s+'          # Preco (8 dígitos)
                r'(?P<estoque>\d{8})\s+'        # Estoque (8 dígitos)
                r'(?P<custo>\d{8})\s+'          # Custo (8 dígitos)
                r'(?P<secao>\d{5})$'            # Secao (5 dígitos)
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
                        'Preco': int(match.group('preco')) / 100,  # Converte para float
                        'Estoque': int(match.group('estoque')),    # Estoque como inteiro
                        'Custo': int(match.group('custo')) / 100,  # Converte para float
                        'Secao': self._remove_leading_zeros(match.group('secao')),
                    })
                except Exception as e:
                    self.logger.error(f"Erro ao processar linha {line_num}: {e}")

            if not data:
                return False, "Nenhum dado válido encontrado."

            # Cria um DataFrame com os dados do TXT
            df = pd.DataFrame(data)

            # Adiciona a coluna Flag com base no arquivo prod_flag.parquet
            df = self._add_flag_data(df)

            # Define a ordem das colunas finais
            columns_order = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque',
                             'Custo', 'Secao', 'Flag']
            df = df[columns_order]

            # Salva os dados no arquivo initial_data.parquet
            output_path = Path(data_path) / "initial_data.parquet"
            df.to_parquet(output_path, index=False)

            # Dispara a combinação de dados em uma thread separada
            threading.Thread(
                target=self._trigger_data_combination,
                args=(data_path,),
                daemon=True
            ).start()

            return True, f"Arquivo processado com sucesso. {len(data)} itens importados."

        except Exception as e:
            self.logger.error(f"Erro ao processar TXT: {e}", exc_info=True)
            return False, f"Erro crítico: {str(e)}"

    
    def _trigger_data_combination(self, data_path: str) -> bool:
        """Dispara a combinação de dados com tratamento de erros."""
        try:
            combiner = DataCombiner(data_path)
            success = combiner.combine_data()

            if success and hasattr(self.inventory_manager, 'notify_ui'):
                self.inventory_manager.notify_ui("data_updated")

            return success
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
        """Processa arquivo Excel no formato específico"""
        try:
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if not data_path:
                return False, "Nenhum inventário ativo selecionado"

            # Carrega a primeira planilha (assumindo que só tem uma)
            df = pd.read_excel(file_path, sheet_name=0)
            
            # Remove linhas totalmente vazias
            df.dropna(how='all', inplace=True)
            
            if len(df) < 1:
                return False, "Planilha vazia ou sem dados válidos"

            # Mapeia colunas
            mapped_df = self._map_excel_columns(df)
            if mapped_df is None:
                return False, "Colunas obrigatórias não encontradas no arquivo Excel"
            
            # Limpeza e agrupamento dos dados
            cleaned_df = self._clean_excel_data(mapped_df)
            
            # Adiciona colunas padrão que podem estar faltando
            for col in ['Codigo', 'Descricao', 'Preco', 'Custo', 'Secao']:
                if col not in cleaned_df.columns:
                    cleaned_df[col] = None
            
            # Adiciona flags
            cleaned_df = self._add_flag_data(cleaned_df)
            
            # Ordena colunas
            final_cols = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque', 
                        'Custo', 'Secao', 'Flag', 'Endereco', 'Operador']
            cleaned_df = cleaned_df[[col for col in final_cols if col in cleaned_df.columns]]
            
            # Salva os dados
            output_path = Path(data_path) / "initial_data.parquet"
            cleaned_df.to_parquet(output_path, index=False)
            
            return True, f"Excel processado com sucesso. {len(cleaned_df)} itens agrupados."

        except Exception as e:
            self.logger.error(f"Erro ao processar Excel: {e}", exc_info=True)
            return False, f"Erro ao processar Excel: {str(e)}"