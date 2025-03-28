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
                
                # Fallback para utf-8 se confiança baixa ou None
                if result['confidence'] < 0.7 or not result['encoding']:
                    return 'utf-8'
                return result['encoding']
        except Exception as e:
            logging.warning(f"Falha ao detectar encoding: {e}")
            return 'utf-8'  # Default seguro
    
    def process_initial_txt(self, file_path: str) -> Tuple[bool, str]:
        """Processa arquivo TXT inicial com tratamento robusto de erros"""
        try:
            data_path = self.inventory_manager.get_active_inventory_data_path()
            if not data_path:
                return False, "Nenhum inventário ativo selecionado"
            
            # Tenta múltiplas codificações como fallback
            encodings_to_try = [
                self.detect_encoding(file_path),
                'utf-8',
                'latin-1',
                'iso-8859-1',
                'cp1252'
            ]
            
            lines = []
            for encoding in filter(None, set(encodings_to_try)):
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        lines = f.readlines()
                    break
                except UnicodeDecodeError:
                    continue
            
            if not lines:
                return False, "Não foi possível ler o arquivo com as codificações testadas"

            pattern = re.compile(
                r'^(?P<gtin>\d{13})\s+'          # GTIN (13 dígitos)
                r'(?P<codigo>\d{9})\s+'          # Código interno (9 dígitos)
                r'(?P<descricao>.+?)\s+'         # Descrição (captura até os espaços antes do preço)
                r'(?P<preco>\d{8})\s+'           # Preço (8 dígitos)
                r'(?P<desconto>\d{8})\s+'        # Desconto (8 dígitos)
                r'(?P<custo>\d{8})\s+'           # Custo (8 dígitos)
                r'(?P<secao>\d{5})$'             # Seção (5 dígitos)
            )

            data = []
            line_errors = []
            
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue

                match = pattern.match(line)
                if not match:
                    line_errors.append(f"Linha {line_num}: Formato inválido")
                    continue

                try:
                    item = {
                        'GTIN': match.group('gtin'),
                        'Descricao': match.group('descricao').strip(),
                        'Estoque': 0  # Valor padrão
                    }
                    data.append(item)
                except Exception as e:
                    line_errors.append(f"Linha {line_num}: {str(e)}")

            if not data:
                error_msg = "Nenhum dado válido encontrado no arquivo"
                if line_errors:
                    error_msg += "\nErros encontrados:\n" + "\n".join(line_errors[:5])  # Mostra até 5 erros
                return False, error_msg

            df = pd.DataFrame(data)
            
            # Verifica dados críticos antes de salvar
            if df['GTIN'].isnull().any():
                return False, "Alguns GTINs estão vazios/inválidos"
                
            output_path = Path(data_path) / "initial_data.parquet"
            df.to_parquet(output_path, index=False)
            
            # Dispara combinação em thread separada para não bloquear a UI
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
            
            # Verificação inicial do arquivo
            if not os.path.exists(file_path):
                return False, "Arquivo não encontrado"
                
            if os.path.getsize(file_path) == 0:
                return False, "Arquivo vazio"

            # Tenta ler com engine openpyxl como fallback
            try:
                df = pd.read_excel(file_path, usecols="A:D", header=None, engine='openpyxl')
            except:
                df = pd.read_excel(file_path, usecols="A:D", header=None)

            if df.shape[1] < 4:
                return False, "O arquivo deve conter pelo menos 4 colunas"
            
            # Limpeza e validação
            df.columns = ['LOJA_KEY', 'OPERADOR', 'ENDERECO', 'COD_BARRAS']
            df['QNT_CONTADA'] = 1
            
            # Remove linhas completamente vazias
            df = df.dropna(how='all')
            
            # Validação de dados
            if df['COD_BARRAS'].isnull().all():
                return False, "Nenhum código de barras válido encontrado"
                
            # Conversão segura de tipos
            df = df.assign(
                LOJA_KEY=df['LOJA_KEY'].astype(str).str.strip(),
                OPERADOR=df['OPERADOR'].astype(str).str.strip(),
                ENDERECO=df['ENDERECO'].astype(str).str.strip(),
                COD_BARRAS=df['COD_BARRAS'].astype(str).str.strip(),
                QNT_CONTADA=pd.to_numeric(df['QNT_CONTADA'], errors='coerce').fillna(1).astype(int)
            )
            
            # Filtra linhas com código de barras inválido
            df = df[df['COD_BARRAS'].str.match(r'^\d+$')]
            
            if df.empty:
                return False, "Nenhum dado válido após filtragem"
            
            # Salva com timestamp e hash para evitar sobreposição
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = Path(data_path) / f"contagem_{timestamp}_{abs(hash(str(df.shape)))}.parquet"
            df.to_parquet(output_path, index=False)
            
            # Dispara combinação em background
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
        """Dispara combinação com tratamento de erros robusto"""
        try:
            combiner = DataCombiner(data_path)
            success = combiner.combine_data()
            
            if success:
                self.logger.info("Combinação automática realizada com sucesso")
                # Notifica a UI se necessário
                if hasattr(self.inventory_manager, 'notify_ui'):
                    self.inventory_manager.notify_ui("data_updated")
            else:
                self.logger.warning("Combinação automática falhou")
                
            return success
        except Exception as e:
            self.logger.error(f"Falha na combinação automática: {e}", exc_info=True)
            return False