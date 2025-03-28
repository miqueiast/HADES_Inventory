import re
import pandas as pd
from typing import Tuple, Optional
import logging

class FileProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.produtos_atencao = None
        
        # Padrão regex atualizado
        self.txt_pattern = re.compile(
            r'^(?P<gtin>\d{13})\s+'          # GTIN (13 dígitos)
            r'(?P<codigo>\S+)\s+'            # Código interno
            r'(?P<descricao>.+?)(?=\s+000)|(?P<descricao2>.+?)(?=\s{2,})'  # Descrição até encontrar " 000" ou múltiplos espaços
            r'(?:\s+000)?\s+'                # Consome o padrão de término
            r'(?P<preco>\d{8})\s+'           # Preço (8 dígitos)
            r'(?P<desconto>\d{8})\s+'        # Desconto (8 dígitos)
            r'(?P<custo>\d{8})\s+'           # Custo (8 dígitos)
            r'(?P<secao>\d{5})$'             # Seção (5 dígitos)
        )

    def process_inventory_txt(self, file_path: str) -> Tuple[bool, Optional[pd.DataFrame], Optional[str]]:
        """Processa arquivo TXT com tratamento especial para descrição e zeros"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            
            data = []
            line_count = 0
            errors = 0
            
            for line in lines:
                line_count += 1
                line = line.strip()
                if not line:
                    continue
                
                match = self.txt_pattern.match(line)
                if not match:
                    errors += 1
                    self.logger.warning(f"Linha {line_count} não corresponde ao padrão: {line[:50]}...")
                    continue
                
                # Combina os dois grupos de descrição possíveis
                descricao = match.group('descricao') or match.group('descricao2')
                
                item = {
                    'GTIN': match.group('gtin'),
                    'Código Interno': match.group('codigo').lstrip('0'),  # Remove zeros à esquerda
                    'Descrição': descricao.strip(),
                    'Preço': float(match.group('preco')) / 100,
                    'Desconto': float(match.group('desconto')) / 100,
                    'Custo': float(match.group('custo')) / 100,
                    'Seção': match.group('secao').lstrip('0'),  # Remove zeros à esquerda
                    'Linha Original': line
                }
                data.append(item)
            
            if not data:
                return False, None, "Nenhum dado válido encontrado no arquivo"
            
            df = pd.DataFrame(data)
            
            # Remove zeros à esquerda de todas as colunas string
            str_cols = df.select_dtypes(include='object').columns
            for col in str_cols:
                if col != 'Descrição':  # Não aplicar na descrição
                    df[col] = df[col].astype(str).str.lstrip('0')
            
            self.logger.info(f"Arquivo processado com {len(df)} itens válidos e {errors} erros")
            return True, df, None
            
        except Exception as e:
            error_msg = f"Erro ao processar TXT: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, None, error_msg

    def save_to_parquet(self, df: pd.DataFrame, output_dir: str, file_name: str) -> Tuple[bool, str]:
        """Salva DataFrame em Parquet removendo zeros à esquerda"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{file_name}.parquet")
            
            # Remove zeros à esquerda antes de salvar
            str_cols = df.select_dtypes(include='object').columns
            for col in str_cols:
                if col != 'Descrição':  # Preserva a descrição original
                    df[col] = df[col].astype(str).str.lstrip('0')
            
            # Tipos de dados otimizados
            dtype_mapping = {
                'GTIN': 'string',
                'Código Interno': 'string',
                'Descrição': 'string',
                'Seção': 'string',
                'Linha Original': 'string'
            }
            
            for col, dtype in dtype_mapping.items():
                if col in df.columns:
                    df[col] = df[col].astype(dtype)
            
            df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            return True, output_path
            
        except Exception as e:
            error_msg = f"Erro ao salvar Parquet: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, error_msg