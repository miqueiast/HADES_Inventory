import pandas as pd
import re
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
import logging
import os
import chardet
from datetime import datetime
import threading
import tkinter as tk
from tkinter import messagebox

from .data_combiner import DataCombiner

class FileProcessor:
    def __init__(self, inventory_manager):
        self.inventory_manager = inventory_manager
        self.logger = logging.getLogger(__name__)
    
    @staticmethod
    def detect_encoding(file_path: str) -> Optional[str]:
        """Detecta a codificação de um arquivo com fallback inteligente"""
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
        """Remove zeros à esquerda de uma string numérica"""
        return value.lstrip('0') or '0'
    
    def _add_flag_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona dados de flag com tratamento para formato atual"""
        try:
            flag_file = Path(__file__).parent / 'prod_flag.parquet'
            
            if not flag_file.exists():
                self.logger.warning("Arquivo prod_flag.parquet não encontrado")
                df['Flag'] = 'Sem flag'
                return df
            
            # Lê o arquivo tratando as colunas concatenadas
            flag_df = pd.read_parquet(flag_file)
            
            # Verifica se é o formato com colunas concatenadas
            if len(flag_df.columns) == 1 and ';' in flag_df.columns[0]:
                self.logger.info("Detectado formato com colunas concatenadas - processando...")
                
                # Extrai as colunas do formato atual
                cols = flag_df.columns[0].split(';')
                flag_df[cols] = flag_df[flag_df.columns[0]].str.split(';', expand=True)
                flag_df = flag_df.drop(flag_df.columns[0], axis=1)
                
                # Padroniza os nomes das colunas
                flag_df = flag_df.rename(columns={
                    'gtin_principal': 'gtin_principal',
                    'flag': 'Flag',
                    'produto_key': 'produto_key'
                })
            else:
                self.logger.info("Formato padrão detectado - processando normalmente")
            
            # Verifica colunas disponíveis após tratamento
            available_columns = flag_df.columns.tolist()
            self.logger.info(f"Colunas disponíveis após tratamento: {available_columns}")
            
            # Mapeamento flexível de colunas
            col_mapping = {
                'gtin': ['gtin_principal', 'gtin', 'codigo_barras', 'ean'],
                'produto': ['produto_key', 'produto', 'codigo', 'sku'],
                'flag': ['Flag', 'flag', 'status']
            }
            
            # Encontra as colunas correspondentes
            gtin_col = next((c for c in col_mapping['gtin'] if c in available_columns), None)
            produto_col = next((c for c in col_mapping['produto'] if c in available_columns), None)
            flag_col = next((c for c in col_mapping['flag'] if c in available_columns), None)
            
            if not gtin_col or not produto_col or not flag_col:
                self.logger.error("Estrutura do arquivo de flags inválida após tratamento")
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
            
            # Limpeza pós-merge
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
        """Processa arquivo TXT inicial com todos os tratamentos"""
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
                r'^(?P<gtin>\d{13})\s+'
                r'(?P<codigo>\d{9})\s+'
                r'(?P<descricao>.+?)\s+'
                r'(?P<preco>\d{8})\s+'
                r'(?P<desconto>\d{8})\s+'
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
                    continue

                try:
                    data.append({
                        'GTIN': self._remove_leading_zeros(match.group('gtin')),
                        'Codigo': self._remove_leading_zeros(match.group('codigo')),
                        'Descricao': match.group('descricao').strip(),
                        'Preco': int(match.group('preco')) / 100,
                        'Desconto': int(match.group('desconto')) / 100,
                        'Custo': int(match.group('custo')) / 100,
                        'Secao': match.group('secao'),
                        'Estoque': 0
                    })
                except Exception:
                    continue

            if not data:
                return False, "Nenhum dado válido encontrado"

            # Cria DataFrame e adiciona flags
            df = pd.DataFrame(data)
            df = self._add_flag_data(df)
            
            # Ordenação das colunas
            columns_order = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Desconto', 
                           'Custo', 'Secao', 'Estoque', 'Flag']
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

    def process_excel_file(self, file_path: str) -> Tuple[bool, str]:
        """Processa arquivo Excel com validação reforçada"""
        try:
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if not data_path:
                return False, "Nenhum inventário ativo selecionado"
            
            # Leitura do arquivo
            try:
                df = pd.read_excel(file_path, usecols="A:D", header=None, engine='openpyxl')
            except:
                df = pd.read_excel(file_path, usecols="A:D", header=None)

            if df.shape[1] < 4:
                return False, "O arquivo deve conter pelo menos 4 colunas"
            
            # Processamento dos dados
            df.columns = ['LOJA_KEY', 'OPERADOR', 'ENDERECO', 'COD_BARRAS']
            df['QNT_CONTADA'] = 1
            
            # Limpeza e conversão
            df = df.dropna(subset=['COD_BARRAS'])
            df['COD_BARRAS'] = df['COD_BARRAS'].astype(str).str.strip()
            df = df[df['COD_BARRAS'].str.match(r'^\d+$')]
            
            if df.empty:
                return False, "Nenhum dado válido após filtragem"
            
            # Salva com timestamp único
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = Path(data_path) / f"contagem_{timestamp}.parquet"
            df.to_parquet(output_path, index=False)
            
            # Dispara combinação
            threading.Thread(
                target=self._trigger_data_combination,
                args=(data_path,),
                daemon=True
            ).start()
            
            return True, f"Dados processados: {len(df)} registros importados"

        except Exception as e:
            self.logger.error(f"Erro ao processar Excel: {e}", exc_info=True)
            return False, f"Erro ao processar: {str(e)}"

    def _trigger_data_combination(self, data_path: str) -> bool:
        """Dispara combinação com tratamento de erros"""
        try:
            combiner = DataCombiner(data_path)
            success = combiner.combine_data()
            
            if success and hasattr(self.inventory_manager, 'notify_ui'):
                self.inventory_manager.notify_ui("data_updated")
                
            return success
        except Exception as e:
            self.logger.error(f"Falha na combinação automática: {e}", exc_info=True)
            return False