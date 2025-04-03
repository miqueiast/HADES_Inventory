import pandas as pd
import os
from pathlib import Path
import time
from typing import Optional, Dict, List
import logging
import threading

class DataCombiner:
    def __init__(self, data_folder: str):
        self.data_folder = data_folder
        self.watching = False
        self.watcher_thread = None
        self.combined_file = "combined_data.parquet"
        self.logger = logging.getLogger(__name__)
        
    def combine_data(self) -> bool:
        """Combina dados de diferentes fontes de forma robusta"""
        try:
            data_path = Path(self.data_folder)
            
            # Carrega dados iniciais ou cria estrutura vazia
            initial_data_path = data_path / "initial_data.parquet"
            if not initial_data_path.exists():
                self.logger.warning("Arquivo inicial não encontrado - criando estrutura vazia")
                df_initial = self._create_empty_initial_data()
                df_initial.to_parquet(initial_data_path)
            else:
                df_initial = pd.read_parquet(initial_data_path)
                # Garante que as colunas necessárias existam
                if 'GTIN' not in df_initial.columns:
                    raise ValueError("Arquivo initial_data.parquet não contém coluna GTIN")
            
            # Processa arquivos de contagem
            count_files = list(data_path.glob("contagem_*.parquet"))
            if count_files:
                # Carrega e concatena todos os arquivos de contagem
                dfs = []
                for file in count_files:
                    try:
                        df = pd.read_parquet(file)
                        if 'COD_BARRAS' in df.columns:
                            dfs.append(df)
                    except Exception as e:
                        self.logger.error(f"Erro ao ler {file.name}: {e}")
                
                if dfs:
                    df_counts = pd.concat(dfs, ignore_index=True)
                    
                    # Agrupa por código de barras
                    grouped = df_counts.groupby('COD_BARRAS').agg({
                        'QNT_CONTADA': 'sum',
                        'OPERADOR': lambda x: ', '.join(set(x.astype(str))),
                        'ENDERECO': lambda x: ', '.join(set(x.astype(str)))
                    }).reset_index()
                    
                    # Merge com dados iniciais
                    merged = pd.merge(
                        df_initial,
                        grouped,
                        how='left',
                        left_on='GTIN',
                        right_on='COD_BARRAS'
                    )
                else:
                    self.logger.warning("Nenhum arquivo de contagem válido encontrado")
                    merged = df_initial.copy()
                    merged['QNT_CONTADA'] = 0
                    merged['OPERADOR'] = ''
                    merged['ENDERECO'] = ''
                    merged['COD_BARRAS'] = merged['GTIN']
            else:
                self.logger.info("Nenhum arquivo de contagem encontrado")
                merged = df_initial.copy()
                merged['QNT_CONTADA'] = 0
                merged['OPERADOR'] = ''
                merged['ENDERECO'] = ''
                merged['COD_BARRAS'] = merged['GTIN']
            
            # Calcula diferenças e ordena
            if 'Estoque' in merged.columns and 'QNT_CONTADA' in merged.columns:
                merged['DIFERENCA'] = merged['QNT_CONTADA'] - merged['Estoque']
                merged = merged.sort_values('DIFERENCA', ascending=False)
            else:
                self.logger.warning("Colunas necessárias para cálculo de diferença não encontradas")
                merged['DIFERENCA'] = 0
            
            # Salva arquivo combinado
            output_path = data_path / self.combined_file
            merged.to_parquet(output_path, index=False)
            
            self.logger.info(f"Dados combinados salvos em {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao combinar dados: {e}", exc_info=True)
            return False
    
    def _create_empty_initial_data(self) -> pd.DataFrame:
        """Cria estrutura de dados inicial vazia com todas as colunas"""
        return pd.DataFrame({
            'GTIN': [],
            'Codigo': [],
            'Descricao': [],
            'Preco': [],
            'Desconto': [],
            'Custo': [],
            'Secao': [],
            'Estoque': [],
            'Flag': []
        })
    
    def start_watching(self, interval: int = 60) -> None:
        """Inicia monitoramento automático da pasta"""
        if self.watching:
            return
            
        self.watching = True
        self.logger.info(f"Iniciando monitoramento da pasta {self.data_folder}")
        
        def watcher():
            last_run = 0
            while self.watching:
                current_time = time.time()
                if current_time - last_run >= interval:
                    try:
                        self.combine_data()
                    except Exception as e:
                        self.logger.error(f"Erro no watcher: {e}")
                    last_run = current_time
                time.sleep(5)
                
        self.watcher_thread = threading.Thread(target=watcher, daemon=True)
        self.watcher_thread.start()
    
    def stop_watching(self) -> None:
        """Para o monitoramento automático"""
        if self.watching:
            self.watching = False
            if self.watcher_thread:
                self.watcher_thread.join(timeout=1)
            self.logger.info("Monitoramento da pasta parado")