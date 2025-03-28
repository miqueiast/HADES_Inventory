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
        """Combina dados de diferentes fontes, criando estrutura vazia se necessário"""
        try:
            data_path = Path(self.data_folder)
            
            # Verifica se existe initial_data, se não, cria estrutura vazia
            initial_data_path = data_path / "initial_data.parquet"
            if not initial_data_path.exists():
                self.logger.warning("Arquivo inicial não encontrado - criando estrutura vazia")
                df_initial = self._create_empty_initial_data()
                df_initial.to_parquet(initial_data_path)
            else:
                df_initial = pd.read_parquet(initial_data_path)
            
            # Carrega arquivos de contagem ou cria DataFrame vazio
            count_files = list(data_path.glob("contagem_*.parquet"))
            if count_files:
                dfs = [pd.read_parquet(file) for file in count_files]
                df_counts = pd.concat(dfs, ignore_index=True)
                
                # Agrupa por código de barras
                grouped = df_counts.groupby('COD_BARRAS').agg({
                    'QNT_CONTADA': 'sum',
                    'OPERADOR': lambda x: '/'.join(set(x.astype(str))),
                    'ENDERECO': lambda x: '/'.join(set(x.astype(str)))
                }).reset_index()
                
                # Faz o merge com os dados iniciais
                merged = pd.merge(
                    df_initial,
                    grouped,
                    how='left',
                    left_on='GTIN',
                    right_on='COD_BARRAS'
                )
            else:
                self.logger.info("Nenhum arquivo de contagem - usando dados iniciais")
                merged = df_initial.copy()
                merged['QNT_CONTADA'] = 0
                merged['OPERADOR'] = ''
                merged['ENDERECO'] = ''
                merged['COD_BARRAS'] = merged['GTIN']
            
            # Calcula diferença
            merged['DIFERENCA'] = merged['QNT_CONTADA'] - merged['Estoque']
            
            # Ordena por maior diferença
            merged = merged.sort_values('DIFERENCA', ascending=False)
            
            # Salva arquivo combinado
            output_path = data_path / self.combined_file
            merged.to_parquet(output_path, index=False)
            
            self.logger.info(f"Dados combinados salvos em {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao combinar dados: {e}", exc_info=True)
            return False
    
    def _create_empty_initial_data(self) -> pd.DataFrame:
        """Cria estrutura de dados inicial vazia"""
        return pd.DataFrame({
            'GTIN': [],
            'Descricao': [],
            'Estoque': []
        })
    
    def start_watching(self, interval: int = 60) -> None:
        """Inicia observação da pasta para combinar dados automaticamente"""
        if self.watching:
            return
            
        self.watching = True
        self.logger.info(f"Iniciando monitoramento da pasta {self.data_folder}")
        
        def watcher():
            last_run = 0
            while self.watching:
                current_time = time.time()
                if current_time - last_run >= interval:
                    self.combine_data()
                    last_run = current_time
                time.sleep(5)
                
        self.watcher_thread = threading.Thread(target=watcher, daemon=True)
        self.watcher_thread.start()
    
    def stop_watching(self) -> None:
        """Para a observação da pasta"""
        if self.watching:
            self.watching = False
            if self.watcher_thread:
                self.watcher_thread.join(timeout=1)
            self.logger.info("Monitoramento da pasta parado")