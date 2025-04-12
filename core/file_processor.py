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
        Faz o join do DataFrame processado com o arquivo `prod_flag.parquet` usando apenas o código do produto.
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
        """Mapeia colunas do Excel para o formato padrão."""
        mapped_df = pd.DataFrame()
        
        for standard_col, possible_cols in self.excel_column_mapping.items():
            for col in possible_cols:
                if col in df.columns:
                    mapped_df[standard_col] = df[col]
                    break
            else:
                if standard_col in ['Endereco', 'Operador']:
                    # Essas colunas são opcionais
                    continue
                self.logger.warning(f"Coluna {standard_col} não encontrada no Excel")
                mapped_df[standard_col] = None
        
        return mapped_df
    
    def _clean_excel_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e formata os dados do Excel."""
        try:
            # Converte GTIN e Código para string e remove zeros à esquerda
            if 'GTIN' in df.columns:
                df['GTIN'] = df['GTIN'].astype(str).apply(self._remove_leading_zeros)
            if 'Codigo' in df.columns:
                df['Codigo'] = df['Codigo'].astype(str).apply(self._remove_leading_zeros)
            
            # Converte valores numéricos
            numeric_cols = ['Preco', 'Estoque', 'Custo']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if col in ['Preco', 'Custo']:
                        df[col] = df[col].round(2)
            
            # Tratamento especial para Endereco e Operador (concatena múltiplos valores)
            if 'Endereco' in df.columns:
                df['Endereco'] = df['Endereco'].astype(str).str.strip()
                # Agrupa por produto e concatena endereços únicos
                if 'Codigo' in df.columns:
                    enderecos = df.groupby('Codigo')['Endereco'].agg(
                        lambda x: ' / '.join(sorted(set(x.astype(str).str.strip()))))
                    df = df.drop('Endereco', axis=1).merge(
                        enderecos, on='Codigo', how='left')
            
            if 'Operador' in df.columns:
                df['Operador'] = df['Operador'].astype(str).str.strip()
                # Agrupa por produto e concatena operadores únicos
                if 'Codigo' in df.columns:
                    operadores = df.groupby('Codigo')['Operador'].agg(
                        lambda x: ' / '.join(sorted(set(x.astype(str).str.strip()))))
                    df = df.drop('Operador', axis=1).merge(
                        operadores, on='Codigo', how='left')
            
            # Preenche valores vazios
            if 'Descricao' in df.columns:
                df['Descricao'] = df['Descricao'].fillna('').astype(str).str.strip()
            
            if 'Secao' in df.columns:
                df['Secao'] = df['Secao'].astype(str).apply(self._remove_leading_zeros)
            
            return df
        except Exception as e:
            self.logger.error(f"Erro na limpeza dos dados: {str(e)}", exc_info=True)
            raise

    def process_excel(self, file_path: str) -> Tuple[bool, str]:
        """Processa arquivo Excel inicial com tratamento robusto."""
        try:
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if not data_path:
                return False, "Nenhum inventário ativo selecionado"

            # Tenta detectar a planilha correta
            try:
                # Primeiro tenta ler todas as planilhas para encontrar a mais adequada
                excel_file = pd.ExcelFile(file_path)
                sheet_names = excel_file.sheet_names
                
                # Prioriza planilhas com nomes que sugerem dados
                preferred_sheets = ['ESTOQUE', 'INVENTARIO', 'PRODUTOS', 'ITENS']
                selected_sheet = None
                
                for name in preferred_sheets:
                    if name in sheet_names:
                        selected_sheet = name
                        break
                
                # Se não encontrar, usa a primeira planilha
                if not selected_sheet:
                    selected_sheet = sheet_names[0]
                
                # Lê os dados
                df = pd.read_excel(file_path, sheet_name=selected_sheet)
            except Exception as e:
                self.logger.error(f"Falha ao ler Excel: {str(e)}")
                return False, "Formato de arquivo Excel inválido"

            # Verifica se tem dados suficientes
            if len(df) < 1:
                return False, "Planilha vazia ou sem dados válidos"

            # Mapeia colunas
            df = self._map_excel_columns(df)
            
            # Limpa os dados
            df = self._clean_excel_data(df)
            
            # Verifica colunas obrigatórias
            required_cols = ['GTIN', 'Codigo', 'Descricao']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                return False, f"Colunas obrigatórias ausentes: {', '.join(missing_cols)}"

            # Adiciona flags
            df = self._add_flag_data(df)

            # Ordena colunas (incluindo as novas colunas opcionais)
            columns_order = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque', 
                           'Custo', 'Secao', 'Flag', 'Endereco', 'Operador']
            df = df[[col for col in columns_order if col in df.columns]]

            # Salva os dados
            output_path = Path(data_path) / "initial_data.parquet"
            df.to_parquet(output_path, index=False)

            # Dispara combinação em thread separada
            threading.Thread(
                target=self._trigger_data_combination,
                args=(data_path,),
                daemon=True
            ).start()

            return True, f"Excel processado com sucesso. {len(df)} itens importados."

        except Exception as e:
            self.logger.error(f"Erro ao processar Excel: {e}", exc_info=True)
            return False, f"Erro crítico: {str(e)}"