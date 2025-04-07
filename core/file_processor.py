# file_processor.py
import pandas as pd
import re
from pathlib import Path
from typing import Tuple, Optional
import logging
import chardet
from datetime import datetime
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
        """Adiciona dados de flag com tratamento para formato atual."""
        try:
            flag_file = Path(__file__).parent / 'prod_flag.parquet'

            if not flag_file.exists():
                self.logger.warning("Arquivo prod_flag.parquet não encontrado")
                df['Flag'] = 'Sem flag'
                return df

            flag_df = pd.read_parquet(flag_file)

            # Identifica colunas de forma flexível
            col_mapping = {
                'gtin': ['gtin_principal', 'gtin', 'codigo_barras', 'ean'],
                'produto': ['produto_key', 'produto', 'codigo', 'sku'],
                'flag': ['Flag', 'flag', 'status']
            }

            available_columns = flag_df.columns.tolist()
            gtin_col = next((c for c in col_mapping['gtin'] if c in available_columns), None)
            produto_col = next((c for c in col_mapping['produto'] if c in available_columns), None)
            flag_col = next((c for c in col_mapping['flag'] if c in available_columns), None)

            if not gtin_col or not produto_col or not flag_col:
                self.logger.error("Estrutura do arquivo de flags inválida")
                df['Flag'] = 'Sem flag'
                return df

            # Conversão de tipos
            df['GTIN'] = df['GTIN'].astype(str)
            df['Codigo'] = df['Codigo'].astype(str)
            flag_df[gtin_col] = flag_df[gtin_col].astype(str)
            flag_df[produto_col] = flag_df[produto_col].astype(str)

            # Merge dos dados
            merged = pd.merge(
                df,
                flag_df[[gtin_col, produto_col, flag_col]],
                how='left',
                left_on=['GTIN', 'Codigo'],
                right_on=[gtin_col, produto_col]
            )

            # Pós-merge
            merged = merged.drop([gtin_col, produto_col], axis=1, errors='ignore')
            merged['Flag'] = merged[flag_col].fillna('Sem flag')

            if flag_col != 'Flag':
                merged = merged.drop(flag_col, axis=1)

            return merged

        except Exception as e:
            self.logger.error(f"Erro ao adicionar flags: {str(e)}", exc_info=True)
            df['Flag'] = 'Sem flag'
            return df

    def process_initial_txt(self, file_path: str) -> Tuple[bool, str]:
        """Processa arquivo TXT inicial com todos os tratamentos."""
        try:
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if not data_path:
                return False, "Nenhum inventário ativo selecionado"

            # Leitura do arquivo
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
                return False, "Não foi possível ler o arquivo"

            # Expressão regular para parsing
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
                    self.logger.warning(f"Linha {line_num} ignorada: formato inválido")
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
                return False, "Nenhum dado válido encontrado"

            # Cria DataFrame e adiciona flags
            df = pd.DataFrame(data)
            df = self._add_flag_data(df)

            # Ordenação das colunas
            columns_order = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque', 
                             'Custo', 'Secao', 'Flag']
            df = df[columns_order]

            # Salva os dados
            output_path = Path(data_path) / "initial_data.parquet"
            df.to_parquet(output_path, index=False)

            # Dispara combinação
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
        """Dispara combinação com tratamento de erros."""
        try:
            combiner = DataCombiner(data_path)
            success = combiner.combine_data()

            if success and hasattr(self.inventory_manager, 'notify_ui'):
                self.inventory_manager.notify_ui("data_updated")

            return success
        except Exception as e:
            self.logger.error(f"Falha na combinação automática: {e}", exc_info=True)
            return False