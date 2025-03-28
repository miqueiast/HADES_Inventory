import pandas as pd
import re
from pathlib import Path
from typing import Tuple, Optional
import logging
import os

class FileProcessor:
    @staticmethod
    def process_initial_txt(file_path: str, output_folder: str) -> Tuple[bool, str]:
        """Processa arquivo TXT inicial com tratamento de codificação"""
        try:
            # Tenta detectar a codificação do arquivo
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        lines = f.readlines()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                return False, "Não foi possível determinar a codificação do arquivo"

            pattern = re.compile(
                r'^(?P<gtin>\d{13})\s+'          # GTIN (13 dígitos)
                r'(?P<codigo>\d{9})\s+'          # Código interno (9 dígitos)
                r'(?P<descricao>.+?)\s+'         # Descrição
                r'(?P<preco>\d{8})\s+'           # Preço (8 dígitos)
                r'(?P<desconto>\d{8})\s+'        # Desconto (8 dígitos)
                r'(?P<custo>\d{8})\s+'           # Custo (8 dígitos)
                r'(?P<secao>\d{5})$'             # Seção (5 dígitos)
            )

            data = []
            line_count = 0
            errors = 0

            for line in lines:
                line_count += 1
                line = line.strip()
                if not line:
                    continue

                match = pattern.match(line)
                if not match:
                    errors += 1
                    continue

                item = {
                    'GTIN': match.group('gtin'),
                    'Descricao': match.group('descricao').strip(),
                    'Estoque': 0  # Valor padrão
                }
                data.append(item)

            if not data:
                return False, "Nenhum dado válido encontrado no arquivo"

            df = pd.DataFrame(data)
            output_path = Path(output_folder) / "initial_data.parquet"
            df.to_parquet(output_path, index=False)
            
            return True, str(output_path)

        except Exception as e:
            return False, f"Erro ao processar arquivo TXT: {str(e)}"

    @staticmethod
    def process_excel_file(file_path: str, output_folder: str) -> Tuple[bool, str]:
        """Processa arquivo Excel pegando as 4 primeiras colunas"""
        try:
            # Lê o arquivo Excel pegando apenas as 4 primeiras colunas (A, B, C, D)
            df = pd.read_excel(file_path, usecols="A:D", header=None)
            
            # Verifica se tem pelo menos 4 colunas
            if df.shape[1] < 4:
                return False, "O arquivo Excel deve ter pelo menos 4 colunas"
            
            # Renomeia as colunas conforme o padrão que queremos
            df.columns = ['LOJA_KEY', 'OPERADOR', 'ENDERECO', 'COD_BARRAS']
            
            # Adiciona a coluna de quantidade contada (inicialmente com valor 1)
            df['QNT_CONTADA'] = 1
            
            # Converte tipos de dados
            df['LOJA_KEY'] = df['LOJA_KEY'].astype(str).str.strip()
            df['OPERADOR'] = df['OPERADOR'].astype(str).str.strip()
            df['ENDERECO'] = df['ENDERECO'].astype(str).str.strip()
            df['COD_BARRAS'] = df['COD_BARRAS'].astype(str).str.strip()
            df['QNT_CONTADA'] = pd.to_numeric(df['QNT_CONTADA'], errors='coerce').fillna(1).astype(int)
            
            # Remove linhas vazias
            df = df.dropna(how='all')
            
            # Gera nome único para o arquivo
            timestamp = int(time.time())
            output_path = Path(output_folder) / f"contagem_{timestamp}.parquet"
            df.to_parquet(output_path, index=False)
            
            return True, str(output_path)
            
        except Exception as e:
            logging.error(f"Erro ao processar arquivo Excel: {e}")
            return False, str(e)