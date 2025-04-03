#data_combiner.py
import pandas as pd
import os
from pathlib import Path
import time
from typing import Optional, Dict, List, Tuple
import logging
import threading
from datetime import datetime

class DataCombiner:
    def __init__(self, data_folder: str):
        """
        Gerenciador para combinação de dados de inventário com monitoramento automático.
        
        Args:
            data_folder (str): Caminho para a pasta de dados do inventário
        """
        self.data_folder = Path(data_folder)
        self.watching = False
        self.watcher_thread = None
        self.combined_file = "combined_data.parquet"
        self.backup_file = "combined_data.bak"
        self.temp_file = "combined_data.tmp"
        self.logger = logging.getLogger(__name__)
        self.lock = threading.Lock()
        self.last_processed = None
        
        # Configurações padrão
        self.min_interval = 5  # segundos entre verificações
        self.max_retries = 3
        self.retry_delay = 1
        
        # Cria a pasta se não existir
        self.data_folder.mkdir(parents=True, exist_ok=True)

    def combine_data(self) -> bool:
        """
        Combina dados iniciais com arquivos de contagem de forma robusta.
        
        Returns:
            bool: True se a combinação foi bem-sucedida, False caso contrário
        """
        with self.lock:
            try:
                self.logger.info("Iniciando combinação de dados...")
                start_time = time.time()
                
                # Carrega dados iniciais com tratamento de erros
                df_initial = self._load_initial_data()
                if df_initial is None:
                    return False
                
                # Processa arquivos de contagem
                df_counts = self._process_count_files()
                
                # Combina os dados
                merged = self._merge_data(df_initial, df_counts)
                
                # Salva o resultado
                success = self._save_combined_data(merged)
                
                if success:
                    process_time = time.time() - start_time
                    self.logger.info(f"Combinação concluída em {process_time:.2f} segundos")
                    self.last_processed = datetime.now()
                
                return success
                
            except Exception as e:
                self.logger.error(f"Falha crítica na combinação de dados: {str(e)}", exc_info=True)
                return False

    def _load_initial_data(self) -> Optional[pd.DataFrame]:
        """Carrega os dados iniciais com tratamento robusto de erros"""
        initial_path = self.data_folder / "initial_data.parquet"
        
        if not initial_path.exists():
            self.logger.warning("Arquivo inicial não encontrado - criando estrutura vazia")
            return self._create_empty_dataset()
            
        for attempt in range(self.max_retries):
            try:
                df = pd.read_parquet(initial_path)
                
                # Verifica colunas obrigatórias
                required_cols = ['GTIN', 'Codigo', 'Descricao', 'Estoque']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    raise ValueError(f"Colunas obrigatórias ausentes: {missing_cols}")
                
                # Converte tipos de dados
                df['GTIN'] = df['GTIN'].astype(str).str.strip()
                df['Codigo'] = df['Codigo'].astype(str).str.strip()
                
                return df
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Falha ao carregar dados iniciais após {self.max_retries} tentativas")
                    return None
                
                time.sleep(self.retry_delay)
                continue

    def _process_count_files(self) -> pd.DataFrame:
        """Processa todos os arquivos de contagem encontrados"""
        count_files = list(self.data_folder.glob("contagem_*.parquet"))
        
        if not count_files:
            self.logger.info("Nenhum arquivo de contagem encontrado")
            return pd.DataFrame({
                'COD_BARRAS': [],
                'QNT_CONTADA': [],
                'OPERADOR': [],
                'ENDERECO': []
            })
        
        dfs = []
        processed_files = set()
        
        for file in count_files:
            try:
                # Verifica se o arquivo já foi processado
                file_id = f"{file.name}_{file.stat().st_mtime}"
                if file_id in processed_files:
                    continue
                
                df = pd.read_parquet(file)
                
                # Verifica colunas necessárias
                if not {'COD_BARRAS', 'QNT_CONTADA'}.issubset(df.columns):
                    self.logger.warning(f"Arquivo {file.name} não contém colunas necessárias")
                    continue
                
                # Pré-processamento
                df = df[['COD_BARRAS', 'QNT_CONTADA', 'OPERADOR', 'ENDERECO']].copy()
                df['COD_BARRAS'] = df['COD_BARRAS'].astype(str).str.strip()
                df['QNT_CONTADA'] = pd.to_numeric(df['QNT_CONTADA'], errors='coerce').fillna(1)
                
                dfs.append(df)
                processed_files.add(file_id)
                
            except Exception as e:
                self.logger.error(f"Erro ao processar {file.name}: {str(e)}")
                continue
        
        if not dfs:
            return pd.DataFrame({
                'COD_BARRAS': [],
                'QNT_CONTADA': [],
                'OPERADOR': [],
                'ENDERECO': []
            })
            
        # Combina todos os DataFrames
        combined = pd.concat(dfs, ignore_index=True)
        
        # Agrega por código de barras
        grouped = combined.groupby('COD_BARRAS').agg({
            'QNT_CONTADA': 'sum',
            'OPERADOR': lambda x: ', '.join(set(str(op) for op in x if pd.notna(op))),
            'ENDERECO': lambda x: ', '.join(set(str(end) for end in x if pd.notna(end)))
        }).reset_index()
        
        self.logger.info(f"Processados {len(combined)} registros de {len(count_files)} arquivos")
        return grouped

    def _merge_data(self, df_initial: pd.DataFrame, df_counts: pd.DataFrame) -> pd.DataFrame:
        """Combina dados iniciais com contagens"""
        # Garante que Estoque seja numérico
        df_initial['Estoque'] = pd.to_numeric(df_initial['Estoque'], errors='coerce').fillna(0)
        
        # Merge seguro
        merged = pd.merge(
            df_initial,
            df_counts,
            how='left',
            left_on='GTIN',
            right_on='COD_BARRAS',
            suffixes=('', '_count')
        )
        
        # Preenche valores ausentes
        merged['QNT_CONTADA'] = merged['QNT_CONTADA'].fillna(0)
        merged['COD_BARRAS'] = merged['COD_BARRAS'].fillna(merged['GTIN'])
        
        # Calcula diferenças
        merged['DIFERENCA'] = merged['QNT_CONTADA'] - merged['Estoque']
        
        # Ordena por diferença (maiores divergências primeiro)
        merged = merged.sort_values('DIFERENCA', ascending=False)
        
        return merged

    def _save_combined_data(self, df: pd.DataFrame) -> bool:
        """Salva os dados combinados com substituição atômica"""
        temp_path = self.data_folder / self.temp_file
        output_path = self.data_folder / self.combined_file
        backup_path = self.data_folder / self.backup_file
        
        try:
            # Salva em arquivo temporário
            df.to_parquet(temp_path, index=False)
            
            # Cria backup se já existir arquivo combinado
            if output_path.exists():
                os.replace(output_path, backup_path)
            
            # Substitui o arquivo principal
            os.replace(temp_path, output_path)
            
            # Remove backup antigo se existir
            if backup_path.exists():
                backup_path.unlink()
            
            self.logger.info(f"Dados combinados salvos em {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao salvar dados combinados: {str(e)}")
            
            # Tenta restaurar backup em caso de falha
            if backup_path.exists() and not output_path.exists():
                try:
                    os.replace(backup_path, output_path)
                    self.logger.info("Backup restaurado com sucesso")
                except Exception as restore_error:
                    self.logger.error(f"Falha ao restaurar backup: {str(restore_error)}")
            
            return False

    def _create_empty_dataset(self) -> pd.DataFrame:
        """Cria um DataFrame vazio com a estrutura esperada"""
        return pd.DataFrame({
            'GTIN': [],
            'Codigo': [],
            'Descricao': [],
            'Preco': [],
            'Desconto': [],
            'Custo': [],
            'Secao': [],
            'Estoque': [],
            'Flag': [],
            'QNT_CONTADA': [],
            'OPERADOR': [],
            'ENDERECO': [],
            'COD_BARRAS': [],
            'DIFERENCA': []
        })

    def start_watching(self, interval: int = 60) -> None:
        """
        Inicia o monitoramento automático da pasta de dados.
        
        Args:
            interval (int): Intervalo entre verificações em segundos
        """
        if self.watching:
            self.logger.warning("Monitoramento já está ativo")
            return
            
        self.watching = True
        self.logger.info(f"Iniciando monitoramento em {self.data_folder} (intervalo: {interval}s)")
        
        def watcher_loop():
            last_check = 0
            while self.watching:
                try:
                    current_time = time.time()
                    
                    # Verifica se é hora de processar
                    if current_time - last_check >= interval:
                        self.combine_data()
                        last_check = current_time
                    
                    time.sleep(self.min_interval)
                    
                except Exception as e:
                    self.logger.error(f"Erro no loop de monitoramento: {str(e)}")
                    time.sleep(interval)  # Espera antes de tentar novamente
        
        self.watcher_thread = threading.Thread(
            target=watcher_loop,
            name="DataWatcher",
            daemon=True
        )
        self.watcher_thread.start()

    def stop_watching(self) -> None:
        """Para o monitoramento automático da pasta"""
        if not self.watching:
            return
            
        self.watching = False
        self.logger.info("Parando monitoramento...")
        
        if self.watcher_thread:
            self.watcher_thread.join(timeout=5)
            
        self.logger.info("Monitoramento parado com sucesso")

    def get_last_processed_time(self) -> Optional[datetime]:
        """Retorna o horário da última combinação bem-sucedida"""
        return self.last_processed

    def get_data_folder(self) -> Path:
        """Retorna o caminho da pasta de dados sendo monitorada"""
        return self.data_folder