import pandas as pd
from pathlib import Path
import time
from typing import Optional, Dict, List
import logging
import threading
from datetime import datetime

class DataCombiner:
    def __init__(self, data_folder: str):
        """
        Gerenciador avançado para combinação de dados de inventário com monitoramento automático.
        
        Args:
            data_folder: Caminho completo para a pasta de dados do inventário
                        (ex: 'data/nome_inventario/dados')
        """
        self.data_folder = Path(data_folder).absolute()
        self.watching = False
        self.watcher_thread = None
        self.combined_file = "combined_data.parquet"
        self.backup_file = "combined_data.bak"
        self.temp_file = "combined_data.tmp"
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.lock = threading.RLock()  # Usando RLock para operações aninhadas
        self.last_processed = None
        self.last_count_mtime = 0
        self._update_callback = None
        
        # Configurações otimizadas
        self.min_interval = 2  # Intervalo mínimo entre verificações (segundos)
        self.max_retries = 3
        self.retry_delay = 1
        self.file_check_interval = 2  # Verificação de modificação de arquivos
        
        # Colunas padrão
        self.initial_columns = [
            'GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque', 
            'Custo', 'Secao', 'Flag'
        ]
        self.count_columns = [
            'COD_BARRAS', 'QNT_CONTADA', 'OPERADOR', 'ENDERECO'
        ]
        self.final_columns = [
            'GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque', 'Custo',
            'Secao', 'Flag', 'QNT_CONTADA', 'DIFERENCA', 'OPERADOR', 'ENDERECO'
        ]
        
        # Garante estrutura de pastas
        self._ensure_data_folder()
        
        self.logger.info(f"DataCombiner configurado para: {self.data_folder}")

    def set_update_callback(self, callback: callable):
        """Define uma função de callback para ser chamada quando os dados são atualizados"""
        self._update_callback = callback
    
    def _ensure_data_folder(self) -> None:
        """Garante que a pasta de dados existe e é acessível"""
        try:
            self.data_folder.mkdir(parents=True, exist_ok=True)
            # Teste de escrita
            test_file = self.data_folder / ".tmp_test"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            self.logger.critical(f"Falha ao acessar pasta de dados: {e}")
            raise PermissionError(f"Não foi possível acessar a pasta {self.data_folder}") from e

    def _safe_read_parquet(self, path: Path, default_cols: List[str]) -> Optional[pd.DataFrame]:
        """Leitura segura de arquivos parquet com tratamento de erros"""
        for attempt in range(self.max_retries):
            try:
                if not path.exists():
                    return pd.DataFrame(columns=default_cols)
                
                df = pd.read_parquet(path, engine='pyarrow')
                return df.copy()  # Retorna cópia explícita para evitar warnings
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Falha ao ler {path.name} após {self.max_retries} tentativas: {e}")
                    return None
                time.sleep(self.retry_delay)

    def _load_initial_data(self) -> Optional[pd.DataFrame]:
        """Carrega e valida os dados iniciais com tratamento robusto"""
        initial_path = self.data_folder / "initial_data.parquet"
        df = self._safe_read_parquet(initial_path, self.initial_columns)
        
        if df is None:
            return None
            
        # Adiciona colunas faltantes com valores padrão
        for col in self.initial_columns:
            if col not in df.columns:
                default = 0 if col in ['Preco', 'Estoque', 'Custo'] else ''
                df[col] = default
                self.logger.warning(f"Coluna '{col}' ausente - preenchida com valor padrão: {default}")
        
        # Processamento seguro dos dados
        try:
            df['GTIN'] = df['GTIN'].astype(str).str.strip().str.zfill(13)
            df['Codigo'] = df['Codigo'].astype(str).str.strip()
            
            # Conversão segura de valores numéricos
            num_cols = ['Preco', 'Estoque', 'Custo']
            for col in num_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Validação de GTIN
            invalid_gtins = df[~df['GTIN'].str.match(r'^\d{13}$')]
            if not invalid_gtins.empty:
                self.logger.warning(f"GTINs inválidos encontrados: {len(invalid_gtins)}")
            
            self.logger.info(f"Dados iniciais carregados. Itens: {len(df)}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao processar dados iniciais: {e}")
            return None

    def _process_count_file(self) -> Optional[pd.DataFrame]:
        """Processa o arquivo de contagem com verificação de modificação e acumulação de dados"""
        count_path = self.data_folder / "dados123.parquet"
        
        try:
            # Sempre processa o arquivo completo (remove verificação de modificação)
            df = self._safe_read_parquet(count_path, self.count_columns)
            if df is None or df.empty:
                return pd.DataFrame(columns=['GTIN', 'QNT_CONTADA', 'OPERADOR', 'ENDERECO'])
            
            # Processamento robusto
            df = df.copy()
            df['COD_BARRAS'] = (
                df['COD_BARRAS']
                .astype(str)
                .str.strip()
                .str.replace(r'\D+', '', regex=True)
                .str.zfill(13)
            )
            
            df['QNT_CONTADA'] = pd.to_numeric(df['QNT_CONTADA'], errors='coerce').fillna(0).astype(int)
            
            # Agregação dinâmica
            agg_rules: Dict[str, any] = {'QNT_CONTADA': 'sum'}
            for col in ['OPERADOR', 'ENDERECO']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip().replace({'nan': '', 'None': ''})
                    agg_rules[col] = lambda x: '; '.join(filter(None, set(x)))
            
            grouped = (
                df.groupby('COD_BARRAS', as_index=False)
                .agg(agg_rules)
                .rename(columns={'COD_BARRAS': 'GTIN'})
            )
            
            self.logger.info(
                f"Contagem processada. Itens únicos: {len(grouped)}, "
                f"Total contado: {grouped['QNT_CONTADA'].sum():,}"
            )
            return grouped
            
        except Exception as e:
            self.logger.error(f"Erro ao processar contagem: {e}")
            return None

    def _merge_data(self, df_initial: pd.DataFrame, df_counts: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Combina dados iniciais com contagens de forma segura"""
        try:
            if df_initial.empty:
                self.logger.error("Dados iniciais vazios - nada para combinar")
                return None
                
            # Começa com os dados iniciais
            result = df_initial.copy()
            
            # Adiciona colunas de contagem se não existirem
            if 'QNT_CONTADA' not in result.columns:
                result['QNT_CONTADA'] = 0
            if 'DIFERENCA' not in result.columns:
                result['DIFERENCA'] = -result['Estoque']
            if 'OPERADOR' not in result.columns:
                result['OPERADOR'] = ''
            if 'ENDERECO' not in result.columns:
                result['ENDERECO'] = ''
            
            # Se houver contagens, faz o merge
            if df_counts is not None and not df_counts.empty:
                # Merge mantendo todos os dados iniciais
                result = result.merge(
                    df_counts,
                    how='left',
                    on='GTIN',
                    suffixes=('', '_new')
                )
                
                # Atualiza apenas os valores que foram contados
                mask = result['QNT_CONTADA_new'].notna()
                result.loc[mask, 'QNT_CONTADA'] = result.loc[mask, 'QNT_CONTADA_new']
                
                # Atualiza operador e endereço apenas se houver novos valores
                for col in ['OPERADOR', 'ENDERECO']:
                    new_col = f"{col}_new"
                    if new_col in result.columns:
                        result[col] = result[col].mask(
                            result[new_col].notna() & (result[new_col] != ''),
                            result[new_col]
                        )
                
                # Remove colunas temporárias
                result.drop(columns=[c for c in result.columns if c.endswith('_new')], inplace=True)
            
            # Calcula diferença com base nos valores atualizados
            result['DIFERENCA'] = result['QNT_CONTADA'] - result['Estoque']
            
            return result
            
        except Exception as e:
            self.logger.error(f"Erro na combinação: {e}")
            return None

    def _prepare_final_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Prepara os dados finais com validação completa"""
        try:
            # Garante todas as colunas necessárias
            for col in self.final_columns:
                if col not in df.columns:
                    default = '' if col in ['Flag', 'OPERADOR', 'ENDERECO'] else 0
                    df[col] = default
                    self.logger.warning(f"Coluna '{col}' ausente - preenchida com: {default}")
            
            # Ordenação profissional
            sort_key = ['DIFERENCA', 'GTIN'] if 'DIFERENCA' in df.columns else ['GTIN']
            df = df.sort_values(
                sort_key,
                ascending=[False, True],
                na_position='last',
                ignore_index=True
            )
            
            # Conversão final de tipos
            num_cols = ['Preco', 'Custo', 'Estoque', 'QNT_CONTADA', 'DIFERENCA']
            for col in num_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Seleciona apenas colunas desejadas
            return df[self.final_columns].copy()
            
        except Exception as e:
            self.logger.error(f"Erro ao preparar dados finais: {e}")
            return None

    def combine_data(self) -> bool:
        """Executa todo o fluxo de combinação de dados com tratamento robusto"""
        with self.lock:
            self.logger.info("Iniciando combinação de dados...")
            start_time = time.time()
            
            try:
                # Carrega e processa dados
                df_initial = self._load_initial_data()
                if df_initial is None:
                    return False
                    
                df_counts = self._process_count_file()
                
                # Combina dados
                merged = self._merge_data(df_initial, df_counts)
                if merged is None:
                    return False
                
                # Prepara e salva resultado
                final_df = self._prepare_final_data(merged)
                if final_df is None:
                    return False
                
                success = self._save_combined_data(final_df)
                
                if success and self._update_callback is not None:
                    try:
                        self._update_callback()
                    except Exception as e:
                        self.logger.error(f"Erro no callback de atualização: {e}")
                return success
                
            except Exception as e:
                self.logger.error(f"Falha na combinação: {e}", exc_info=True)
                return False
            finally:
                self.logger.debug("Processo de combinação finalizado")

    def _save_combined_data(self, df: pd.DataFrame) -> bool:
        """Salva os dados com substituição atômica e backup seguro"""
        temp_path = self.data_folder / self.temp_file
        final_path = self.data_folder / self.combined_file
        backup_path = self.data_folder / self.backup_file
        
        try:
            # Salva em arquivo temporário
            df.to_parquet(temp_path, index=False, engine='pyarrow')
            
            # Backup atômico
            if final_path.exists():
                backup_path.unlink(missing_ok=True)
                final_path.rename(backup_path)
            
            # Commit final
            temp_path.rename(final_path)
            self.logger.info(f"Dados salvos em {final_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao salvar dados: {e}")
            
            # Recovery attempt
            temp_path.unlink(missing_ok=True)
            if not final_path.exists() and backup_path.exists():
                try:
                    backup_path.rename(final_path)
                    self.logger.warning("Backup restaurado após falha")
                except Exception as restore_error:
                    self.logger.critical(f"Falha ao restaurar backup: {restore_error}")
            
            return False

    def start_watching(self, interval: int = 2) -> None:
        """Inicia monitoramento contínuo com intervalo configurável"""
        if self.watching:
            self.logger.warning("Monitoramento já está ativo")
            return
            
        self.watching = True
        self.logger.info(f"Iniciando monitoramento (intervalo: {interval}s)")
        
        def watcher():
            while self.watching:
                try:
                    self.combine_data()
                    time.sleep(interval)
                except Exception as e:
                    self.logger.error(f"Erro no watcher: {e}")
                    time.sleep(min(30, interval * 2))  # Backoff exponencial
        
        self.watcher_thread = threading.Thread(
            target=watcher,
            name="DataWatcher",
            daemon=True
        )
        self.watcher_thread.start()

    def stop_watching(self) -> None:
        """Para o monitoramento de forma segura"""
        if not self.watching:
            return
            
        self.watching = False
        if self.watcher_thread:
            self.watcher_thread.join(timeout=5)
        self.logger.info("Monitoramento parado")

    def get_last_processed_time(self) -> Optional[datetime]:
        """Retorna o último horário de processamento bem-sucedido"""
        return self.last_processed

    def get_data_folder(self) -> Path:
        """Retorna o caminho absoluto da pasta de dados"""
        return self.data_folder

    def __del__(self):
        """Garante limpeza adequada"""
        self.stop_watching()
        self.logger.info("Instância DataCombiner finalizada")

    def __enter__(self):
        """Suporte para context manager"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Garante parada do watcher ao sair do contexto"""
        self.stop_watching()