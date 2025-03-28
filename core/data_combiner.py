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
        self.prod_flag_file = "prod_flag.parquet"
        
    def combine_data(self) -> bool:
        """Combina dados de diferentes fontes em um único arquivo"""
        try:
            # Carrega dados iniciais
            initial_data_path = Path(self.data_folder) / "initial_data.parquet"
            if not initial_data_path.exists():
                logging.error("Arquivo inicial não encontrado")
                return False
                
            df_initial = pd.read_parquet(initial_data_path)
            
            # Carrega arquivos de contagem
            count_files = list(Path(self.data_folder).glob("contagem_*.parquet"))
            if not count_files:
                logging.warning("Nenhum arquivo de contagem encontrado")
                return False
                
            # Combina todos os arquivos de contagem
            dfs = []
            for file in count_files:
                df = pd.read_parquet(file)
                dfs.append(df)
                
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
            
            # Calcula diferença
            merged['DIFERENCA'] = merged['QNT_CONTADA'] - merged['Estoque']
            
            # Ordena por maior diferença
            merged = merged.sort_values('DIFERENCA', ascending=False)
            
            # Salva arquivo combinado
            output_path = Path(self.data_folder) / self.combined_file
            merged.to_parquet(output_path, index=False)
            
            return True
            
        except Exception as e:
            logging.error(f"Erro ao combinar dados: {e}")
            return False
    
    def start_watching(self, interval: int = 60) -> None:
        """Inicia observação da pasta para combinar dados automaticamente"""
        if self.watching:
            return
            
        self.watching = True
        
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
        self.watching = False
        if self.watcher_thread:
            self.watcher_thread.join(timeout=1)