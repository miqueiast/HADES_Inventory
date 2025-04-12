import pandas as pd
# import os  # Removed as it is not accessed
from pathlib import Path
import time
from typing import Optional  # Removed Dict, List, Tuple as they are not accessed
import logging
import threading
from datetime import datetime
# import shutil  # Removed as it is not accessed

class DataCombiner:
    def __init__(self, data_folder: str):
        """
        Gerenciador para combinação de dados de inventário com monitoramento automático.
        
        Args:
            data_folder (str): Caminho para a pasta base do inventário (contendo a subpasta 'dados')
        """
        self.base_folder = Path(data_folder)
        self.data_folder = self.base_folder / "dados"
        self.watching = False
        self.watcher_thread = None
        self.combined_file = "combined_data.parquet"
        self.backup_file = "combined_data.bak"
        self.temp_file = "combined_data.tmp"
        self.logger = logging.getLogger(f"{__name__}.DataCombiner")
        self.lock = threading.Lock()
        self.last_processed = None
        
        # Configurações
        self.min_interval = 5  # segundos entre verificações
        self.max_retries = 3   # tentativas de leitura de arquivo
        self.retry_delay = 1   # segundos entre tentativas
                
        # Garante a estrutura de pastas
        self._ensure_data_folder()
        # Cria a pasta se não existir
        self.data_folder.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"DataCombiner configurado para: {self.data_folder}")

    def _ensure_data_folder(self):
        """Garante que a pasta de dados existe e é acessível"""
        try:
            pass  # Add your logic here
        except Exception as e:
            self.logger.error(f"Erro no bloco try: {e}")
        except Exception as e:
            self.logger.error(f"Erro no bloco try: {e}")
            self.data_folder.mkdir(parents=True, exist_ok=True)
            # Testa escrita na pasta
            test_file = self.data_folder / ".access_test"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            self.logger.critical(f"Falha ao acessar pasta de dados: {e}")
            raise

    def combine_data(self) -> bool:
        """
        Combina dados iniciais com arquivo de contagem de forma robusta.
        
        Returns:
            bool: True se a combinação foi bem-sucedida, False caso contrário
        """
        with self.lock:
            try:
                self.logger.info("Iniciando combinação de dados...")
                start_time = time.time()
                
                # 1. Carrega dados iniciais
                df_initial = self._load_initial_data()
                if df_initial is None:
                    self.logger.error("Falha ao carregar dados iniciais")
                    return False
                
                if df_initial.empty:
                    self.logger.error("Dados iniciais estão vazios")
                    return False
                
                # 2. Processa arquivo de contagem
                df_counts = self._process_count_file()
                if df_counts is None:
                    self.logger.error("Falha ao processar arquivo de contagem")
                    return False
                
                # 3. Combina os dados
                merged = self._merge_data(df_initial, df_counts)
                if merged is None or merged.empty:
                    self.logger.error("Falha ao combinar dados - resultado vazio")
                    return False
                
                # 4. Prepara colunas finais
                final_df = self._prepare_final_data(merged)
                if final_df is None:
                    return False
                
                # 5. Salva o resultado
                success = self._save_combined_data(final_df)
                
                if success:
                    process_time = time.time() - start_time
                    self.logger.info(
                        f"Combinação concluída com sucesso em {process_time:.2f}s. "
                        f"Total de itens: {len(final_df)}"
                    )
                    self.last_processed = datetime.now()
                    return True
                
                return False
                
            except Exception as e:
                self.logger.error(f"Falha crítica na combinação de dados: {e}", exc_info=True)
                return False
            finally:
                self.logger.info("Finalizado processo de combinação de dados")

    def _load_initial_data(self) -> Optional[pd.DataFrame]:
        """Carrega os dados iniciais (inventário) da pasta 'dados'"""
        initial_path = self.data_folder / "initial_data.parquet"
        
        if not initial_path.exists():
            self.logger.error(f"Arquivo inicial não encontrado: {initial_path}")
            return None
            
        for attempt in range(self.max_retries):
            try:
                # Carrega o arquivo com engine='pyarrow' para melhor performance
                df = pd.read_parquet(initial_path, engine='pyarrow')
                
                # Verifica colunas obrigatórias
                required_cols = ['GTIN', 'Codigo', 'Descricao', 'Estoque']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    self.logger.error(f"Colunas obrigatórias ausentes: {missing_cols}")
                    return None
                
                # Cria cópia explícita para evitar warnings
                df = df.copy()
                
                # Conversão segura de tipos de dados
                df['GTIN'] = df['GTIN'].astype(str).str.strip().str.zfill(13)
                df['Codigo'] = df['Codigo'].astype(str).str.strip()
                df['Estoque'] = pd.to_numeric(df['Estoque'], errors='coerce').fillna(0).astype(int)
                
                # Verifica dados inválidos
                if df['GTIN'].str.len().ne(13).any():
                    invalid_gtins = df[df['GTIN'].str.len() != 13]['GTIN'].unique()
                    self.logger.warning(f"GTINs com formato inválido: {invalid_gtins[:5]}")
                
                self.logger.info(f"Dados iniciais carregados. Itens: {len(df)}")
                return df
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(
                        f"Falha ao carregar dados iniciais após {self.max_retries} tentativas: {e}",
                        exc_info=True
                    )
                    return None
                
                time.sleep(self.retry_delay)

    def _process_count_file(self) -> Optional[pd.DataFrame]:
        """Processa o arquivo de contagem (dados123.parquet) da pasta 'dados'"""
        count_path = self.data_folder / "dados123.parquet"
        
        if not count_path.exists():
            self.logger.warning("Arquivo de contagem não encontrado - usando DataFrame vazio")
            return pd.DataFrame(columns=['COD_BARRAS', 'QNT_CONTADA', 'OPERADOR', 'ENDERECO'])
        
        for attempt in range(self.max_retries):
            try:
                # Carrega com engine específica para evitar warnings
                df = pd.read_parquet(count_path, engine='pyarrow')
                
                if df.empty:
                    self.logger.warning("Arquivo de contagem está vazio")
                    return df
                
                # Verifica colunas mínimas necessárias
                if not {'COD_BARRAS', 'QNT_CONTADA'}.issubset(df.columns):
                    self.logger.error(
                        "Arquivo de contagem não contém colunas obrigatórias (COD_BARRAS, QNT_CONTADA)"
                    )
                    return pd.DataFrame(columns=['COD_BARRAS', 'QNT_CONTADA', 'OPERADOR', 'ENDERECO'])
                
                # Cria cópia explícita para manipulação segura
                df['COD_BARRAS'] = (
                    df['COD_BARRAS']
                    .astype(str)
                    .str.strip()
                    .str.replace(r'\D+', '', regex=True)  # Remove não-dígitos
                    .str.zfill(13)
                )
                
                # Pré-processamento robusto
                df['COD_BARRAS'] = (
                    df['COD_BARRAS']
                    .astype(str)
                    .str.strip()
                    .str.replace(r'\D+', '', regex=True)  # Remove não-dígitos
                    .str.zfill(13)
                )
                
                df['QNT_CONTADA'] = (
                    pd.to_numeric(df['QNT_CONTADA'], errors='coerce')
                    .fillna(0)
                    .astype(int)
                )
                
                # Configura agregação dinâmica
                agg_rules = {'QNT_CONTADA': 'sum'}
                
                # Adiciona colunas opcionais se existirem
                text_cols = ['OPERADOR', 'ENDERECO']
                for col in text_cols:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.strip().replace({'nan': '', 'None': ''})
                        agg_rules[col] = lambda x: '; '.join(filter(None, set(x)))
                
                # Agrupa por código de barras
                grouped = (
                    df.groupby('COD_BARRAS', as_index=False)
                    .agg(agg_rules)
                    .rename(columns={'COD_BARRAS': 'GTIN'})
                )
                
                self.logger.info(
                    f"Arquivo de contagem processado. "
                    f"Itens únicos: {len(grouped)}, "
                    f"Total contado: {grouped['QNT_CONTADA'].sum()}"
                )
                return grouped
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(
                        f"Falha ao processar arquivo de contagem após {self.max_retries} tentativas: {e}",
                        exc_info=True
                    )
                    return pd.DataFrame(columns=['COD_BARRAS', 'QNT_CONTADA', 'OPERADOR', 'ENDERECO'])
                
                time.sleep(self.retry_delay)

    def _merge_data(self, df_initial: pd.DataFrame, df_counts: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Combina dados iniciais com contagens de forma segura"""
        try:
            if df_initial.empty:
                self.logger.error("Dados iniciais vazios - não é possível combinar")
                return None
                
            # Se não houver dados de contagem, cria DataFrame com estrutura básica
            if df_counts.empty:
                self.logger.warning("Sem dados de contagem - usando valores padrão")
                df_counts = pd.DataFrame(columns=['GTIN', 'QNT_CONTADA'])
            
            # Merge seguro (left join) garantindo tipos consistentes
            df_initial['GTIN'] = df_initial['GTIN'].astype(str)
            df_counts['GTIN'] = df_counts['GTIN'].astype(str)
            
            merged = pd.merge(
                df_initial,
                df_counts,
                how='left',
                on='GTIN',
                suffixes=('', '_count')
            )
            
            # Pós-processamento
            return self._post_process_merged_data(merged)
            
        except Exception as e:
            self.logger.error(f"Erro ao combinar dados: {e}", exc_info=True)
            return None

    def _post_process_merged_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Realiza pós-processamento dos dados combinados"""
        # Cria cópia explícita para evitar warnings
        df = df.copy()
        
        # Preenche valores ausentes de forma segura
        df['QNT_CONTADA'] = df['QNT_CONTADA'].fillna(0).astype(int)
        
        # Colunas opcionais
        for col in ['OPERADOR', 'ENDERECO']:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
        
        # Calcula diferenças
        if 'Estoque' in df.columns and 'QNT_CONTADA' in df.columns:
            df['DIFERENCA'] = df['QNT_CONTADA'] - df['Estoque'].astype(int)
        
        # Ordenação segura
        sort_col = 'DIFERENCA' if 'DIFERENCA' in df.columns else 'GTIN'
        try:
            return df.sort_values(
                sort_col,
                ascending=False if sort_col == 'DIFERENCA' else True,
                na_position='last',
                ignore_index=True
            )
        except Exception as e:
            self.logger.warning(f"Erro ao ordenar por {sort_col}: {e}")
            return df

    def _prepare_final_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Prepara os dados finais com colunas padronizadas"""
        try:
            # Colunas esperadas no resultado final
            final_columns = [
                'GTIN', 'Codigo', 'Descricao', 'Preco', 'Custo', 'Estoque',
                'QNT_CONTADA', 'Flag', 'DIFERENCA', 'OPERADOR', 'ENDERECO'
            ]
            
            # Garante que todas as colunas existam
            for col in final_columns:
                if col not in df.columns:
                    default_value = '' if col in ['Flag', 'OPERADOR', 'ENDERECO'] else 0
                    df[col] = default_value
                    self.logger.warning(f"Coluna {col} não encontrada - preenchida com {default_value}")
            
            # Seleciona e ordena as colunas
            df = df[final_columns].copy()
            
            # Conversão final de tipos
            numeric_cols = ['Preco', 'Custo', 'Estoque', 'QNT_CONTADA', 'DIFERENCA']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao preparar dados finais: {e}", exc_info=True)
            return None

    def _save_combined_data(self, df: pd.DataFrame) -> bool:
        """Salva os dados combinados com substituição atômica"""
        temp_path = self.data_folder / self.temp_file
        output_path = self.data_folder / self.combined_file
        backup_path = self.data_folder / self.backup_file
        
        try:
            # 1. Salva em arquivo temporário
            df.to_parquet(temp_path, index=False, engine='pyarrow')
            
            # 2. Cria backup se o arquivo final já existir
            if output_path.exists():
                try:
                    backup_path.unlink(missing_ok=True)  # Remove backup antigo se existir
                    output_path.rename(backup_path)
                    self.logger.info(f"Backup criado: {backup_path}")
                except Exception as e:
                    self.logger.error(f"Falha ao criar backup: {e}")
                    return False
            
            # 3. Move o arquivo temporário para o final
            temp_path.rename(output_path)
            self.logger.info(f"Dados combinados salvos em: {output_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao salvar dados combinados: {e}", exc_info=True)
            
            # Tenta restaurar backup em caso de falha
            if backup_path.exists() and not output_path.exists():
                try:
                    backup_path.rename(output_path)
                    self.logger.info("Backup restaurado com sucesso após falha")
                except Exception as e:
                    self.logger.error(f"Falha ao restaurar backup: {e}")
            
            # Limpa arquivo temporário se existir
            temp_path.unlink(missing_ok=True)
            return False

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
                    self.logger.error(f"Erro no loop de monitoramento: {e}")
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
        self.stop_watching()
        """Retorna o horário da última combinação bem-sucedida"""
        return self.last_processed

    def get_data_folder(self) -> Path:
        """Retorna o caminho da pasta de dados sendo monitorada"""
        return self.data_folder

    def __del__(self):
        """Destruidor - garante que o watcher seja parado"""
        self.stop_watching()