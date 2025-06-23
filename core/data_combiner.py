import pandas as pd
from pathlib import Path
import time
from typing import Optional, Dict, List, Callable, Any # [CITE: 1] <-- Adicione Any para tipagem flexível
import logging
import threading

class DataCombiner:
    """
    Gerenciador avançado para combinação de dados de inventário com monitoramento automático.
    Combina dados de múltiplas fontes de contagem (manual, API, etc.), descobrindo
    arquivos dinamicamente e garantindo a padronização das chaves de junção.
    """
    def __init__(self, data_folder: str):
        self.data_folder = Path(data_folder).absolute()
        self.watching = False
        self.watcher_thread = None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.lock = threading.RLock()
        self._update_callback: Optional[Callable[[], None]] = None
        
        self.static_count_files = ["api_counts.parquet", "manual_counts.parquet"] # [CITE: 1] <-- ADIÇÃO: Inclui manual_counts.parquet
        self.dynamic_count_patterns = ["contagem_*.parquet"] # [CITE: 1] <-- REMOVIDO: "manual_*.parquet" pois agora temos um nome fixo
        
        self.combined_file = self.data_folder / "combined_data.parquet"
        self.backup_file = self.data_folder / "combined_data.bak"
        self.temp_file = self.data_folder / "combined_data.tmp"

        self.max_retries = 3
        self.retry_delay = 1
        
        self.initial_columns = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque', 'Custo', 'Secao', 'Flag']
        self.count_columns = ['COD_BARRAS', 'QNT_CONTADA', 'OPERADOR', 'ENDERECO', 'LOJA_KEY'] # [CITE: 1] <-- ADIÇÃO: LOJA_KEY
        self.final_columns = ['GTIN', 'Codigo', 'Descricao', 'Preco', 'Estoque', 'Custo', 'Secao', 'Flag', 'QNT_CONTADA', 'DIFERENCA', 'OPERADOR', 'ENDERECO']
        
        self._ensure_data_folder()
        self.logger.info(f"DataCombiner configurado para a pasta: {self.data_folder}")

    ### CORREÇÃO PRINCIPAL 1: NOVO MÉTODO DE PADRONIZAÇÃO ###
    def _standardize_barcode(self, series: pd.Series) -> pd.Series:
        """
        Aplica uma limpeza rigorosa e padroniza uma série de códigos de barra para o formato GTIN-13.
        Esta é a função chave para garantir que o merge funcione corretamente.
        """
        # [CITE: 1] Adicionado .fillna('') para garantir que nulos sejam tratados como strings vazias antes da limpeza.
        return (
            series.astype(str)                               # 1. Garante que tudo é texto
            .fillna('')                                      # 1.5. Trata NaN como string vazia
            .str.strip()                                     # 2. Remove espaços em branco no início e fim
            .str.replace(r'\D', '', regex=True)              # 3. Remove QUALQUER caractere que não seja um dígito
            .str.zfill(13)                                   # 4. Adiciona zeros à esquerda para completar 13 dígitos
        )

    def set_update_callback(self, callback: Callable[[], None]):
        self._update_callback = callback
    
    def _ensure_data_folder(self):
        try:
            self.data_folder.mkdir(parents=True, exist_ok=True)
            (self.data_folder / ".write_test").touch()
            (self.data_folder / ".write_test").unlink()
        except Exception as e:
            self.logger.critical(f"Falha ao acessar ou criar a pasta de dados: {e}")
            raise PermissionError(f"Não foi possível acessar a pasta {self.data_folder}") from e

    def _safe_read_parquet(self, path: Path) -> Optional[pd.DataFrame]:
        if not path.exists(): return None
        for attempt in range(self.max_retries):
            try:
                # [CITE: 1] Adicionado try-except para o .copy() também, mais defensivo
                df = pd.read_parquet(path, engine='pyarrow')
                return df.copy()
            except Exception as e:
                self.logger.warning(f"Tentativa {attempt + 1} de ler {path.name} falhou: {e}")
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Falha final ao ler {path.name}: {e}")
                    return None
                time.sleep(self.retry_delay)
        return None

    def _load_initial_data(self) -> Optional[pd.DataFrame]:
        initial_path = self.data_folder / "initial_data.parquet"
        df = self._safe_read_parquet(initial_path)
        
        if df is None:
            self.logger.warning("Arquivo de dados inicial 'initial_data.parquet' não encontrado. Retornando DataFrame vazio.")
            # [CITE: 1] Retorna um DataFrame vazio com as colunas iniciais para garantir a estrutura
            return pd.DataFrame(columns=self.initial_columns) 
        
        for col in self.initial_columns:
            if col not in df.columns:
                df[col] = 0 if col in ['Preco', 'Estoque', 'Custo'] else ''
        
        try:
            ### CORREÇÃO PRINCIPAL 2: APLICANDO A PADRONIZAÇÃO NO GTIN DA INICIAL ###
            # É crucial que o GTIN na base inicial esteja padronizado para o merge funcionar.
            # Se 'GTIN' não existe ou não está em string, este ajuste garante a padronização.
            if 'GTIN' in df.columns: # [CITE: 1] Adicionado para verificar se a coluna existe
                df['GTIN'] = self._standardize_barcode(df['GTIN'])
            else: # [CITE: 1] Se 'GTIN' não existe, crie-a como string vazia padronizada.
                df['GTIN'] = self._standardize_barcode(pd.Series([''])) # Cria uma série vazia e padroniza
            
            df['Codigo'] = df['Codigo'].astype(str).str.strip()
            for col in ['Preco', 'Estoque', 'Custo']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            self.logger.info(f"Dados iniciais carregados com {len(df)} itens.")
            return df[self.initial_columns]
        except Exception as e:
            self.logger.error(f"Erro ao processar dados iniciais: {e}", exc_info=True)
            return None

    def _load_all_count_data(self) -> Optional[pd.DataFrame]:
        all_counts_dfs = []
        
        # [CITE: 1] Carrega arquivos de contagem estáticos (inclui api_counts.parquet e manual_counts.parquet)
        for filename in self.static_count_files:
            file_path = self.data_folder / filename
            if file_path.exists():
                df_source = self._safe_read_parquet(file_path)
                if df_source is not None and not df_source.empty:
                    # [CITE: 1] ADIÇÃO: Padroniza COD_BARRAS para todos os arquivos de contagem
                    if 'COD_BARRAS' in df_source.columns:
                        df_source['COD_BARRAS'] = self._standardize_barcode(df_source['COD_BARRAS'])
                    else: # [CITE: 1] Se 'COD_BARRAS' não existe, loga e pula ou cria coluna vazia
                        self.logger.warning(f"Coluna 'COD_BARRAS' não encontrada em {filename}. Pulando este arquivo de contagem.")
                        continue # Pula este arquivo se a chave principal não existe
                    all_counts_dfs.append(df_source)

        # [CITE: 1] Carrega arquivos de contagem dinâmicos (e.g., contagem_*.parquet)
        for pattern in self.dynamic_count_patterns:
            for file_path in self.data_folder.glob(pattern):
                df_source = self._safe_read_parquet(file_path)
                if df_source is not None and not df_source.empty:
                    # [CITE: 1] ADIÇÃO: Padroniza COD_BARRAS para todos os arquivos de contagem
                    if 'COD_BARRAS' in df_source.columns:
                        df_source['COD_BARRAS'] = self._standardize_barcode(df_source['COD_BARRAS'])
                    else: # [CITE: 1] Se 'COD_BARRAS' não existe, loga e pula ou cria coluna vazia
                        self.logger.warning(f"Coluna 'COD_BARRAS' não encontrada em {file_path.name}. Pulando este arquivo de contagem.")
                        continue # Pula este arquivo se a chave principal não existe
                    all_counts_dfs.append(df_source)

        if not all_counts_dfs:
            self.logger.info("Nenhum arquivo de contagem válido encontrado para processar.")
            # [CITE: 1] Retorna um DataFrame vazio com as colunas esperadas para contagem
            return pd.DataFrame(columns=['COD_BARRAS'] + [col for col in self.count_columns if col != 'COD_BARRAS']) 

        df = pd.concat(all_counts_dfs, ignore_index=True)
        self.logger.info(f"Total de {len(df)} registros de contagem combinados de {len(all_counts_dfs)} fontes.")
        
        try:
            # [CITE: 1] Garante que todas as colunas de contagem existam antes de processar
            for col in self.count_columns:
                if col not in df.columns: 
                    df[col] = 0 if col == 'QNT_CONTADA' else ''

            df['QNT_CONTADA'] = pd.to_numeric(df['QNT_CONTADA'], errors='coerce').fillna(0)
            
            # [CITE: 1] Regras de agregação para QNT_CONTADA, OPERADOR e ENDERECO
            agg_rules: Dict[str, Any] = {'QNT_CONTADA': 'sum'} # [CITE: 1] Alterado para Any
            for col in ['OPERADOR', 'ENDERECO', 'LOJA_KEY']: # [CITE: 1] ADIÇÃO: LOJA_KEY na agregação
                if col in df.columns: # [CITE: 1] Verifica se a coluna existe antes de adicionar a regra
                    df[col] = df[col].astype(str).str.strip().replace({'nan': '', 'None': ''}) # [CITE: 1] Limpa strings
                    # [CITE: 1] Função lambda melhorada para combinar strings e garantir que não haja vazios
                    agg_rules[col] = lambda x: '; '.join(filter(None, sorted(set(str(val) for val in x if pd.notna(val) and str(val).strip()))))
            
            # [CITE: 1] Agrupa por COD_BARRAS E LOJA_KEY para maior precisão se o mesmo COD_BARRAS aparecer em lojas diferentes
            grouped = df.groupby(['COD_BARRAS', 'LOJA_KEY'], as_index=False).agg(agg_rules).rename(columns={'COD_BARRAS': 'GTIN'})
            
            self.logger.info(f"Dados de contagem consolidados em {len(grouped)} itens únicos (por GTIN e LOJA_KEY).")
            return grouped
        except Exception as e:
            self.logger.error(f"Erro ao processar dados de contagem consolidados: {e}", exc_info=True)
            return None

    def _merge_data(self, df_initial: pd.DataFrame, df_counts: pd.DataFrame) -> Optional[pd.DataFrame]:
        try:
            if df_initial.empty:
                self.logger.warning("Dados iniciais vazios. O resultado conterá apenas as contagens.")
                # [CITE: 1] Garante que df_counts tenha as colunas finais esperadas, preenchendo com vazios/zeros
                final_df = df_counts.copy()
                for col in self.final_columns:
                    if col not in final_df.columns:
                        final_df[col] = 0 if col in ['Estoque', 'Preco', 'Custo', 'QNT_CONTADA', 'DIFERENCA'] else ''
                return final_df[self.final_columns] # [CITE: 1] Retorna com a ordem de colunas final
            
            if df_counts.empty: # [CITE: 1] Se não há contagens, retorna os dados iniciais com contagens zeradas
                self.logger.info("Nenhum dado de contagem para mesclar. Retornando dados iniciais com contagens zeradas.")
                df_initial['QNT_CONTADA'] = 0
                df_initial['OPERADOR'] = ''
                df_initial['ENDERECO'] = ''
                df_initial['DIFERENCA'] = df_initial['QNT_CONTADA'] - df_initial['Estoque']
                return df_initial[self.final_columns]

            # [CITE: 1] O merge agora funcionará corretamente porque os GTINs (e LOJA_KEY se for usada) estão padronizados
            # Usa 'GTIN' e 'LOJA_KEY' (se disponível nos dois) para um merge mais preciso
            
            # [CITE: 1] Prepara chaves de merge
            merge_on_cols = ['GTIN']
            # if 'LOJA_KEY' in df_initial.columns and 'LOJA_KEY' in df_counts.columns:
            #     merge_on_cols.append('LOJA_KEY')
            # [CITE: 1] A LOJA_KEY em initial_data.parquet geralmente não existe.
            # Vamos manter o merge principal por GTIN.
            # A filtragem por LOJA_KEY já acontece antes de salvar api_counts e manual_counts.

            result = pd.merge(df_initial, df_counts, how='left', on='GTIN')
            
            # [CITE: 1] Preenche NaNs resultantes do merge e garante o tipo de dado correto
            result['QNT_CONTADA'] = result['QNT_CONTADA'].fillna(0).astype(int)
            for col in ['OPERADOR', 'ENDERECO']:
                if col in result.columns:
                    result[col] = result[col].fillna('').astype(str)
            
            # [CITE: 1] Recalcula a diferença (assegura que Estoque é int antes do cálculo)
            result['Estoque'] = pd.to_numeric(result['Estoque'], errors='coerce').fillna(0).astype(int)
            result['DIFERENCA'] = result['QNT_CONTADA'] - result['Estoque']
            
            return result
        except Exception as e:
            self.logger.error(f"Erro ao mesclar dados: {e}", exc_info=True)
            return None

    def _prepare_final_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        try:
            # [CITE: 1] Garante que todas as colunas finais existam antes de reordenar
            for col in self.final_columns:
                if col not in df.columns:
                    df[col] = 0 if col in ['Estoque', 'QNT_CONTADA', 'DIFERENCA', 'Preco', 'Custo'] else ''
            
            df = df.sort_values(by=['DIFERENCA'], key=lambda x: abs(x), ascending=False, na_position='last', ignore_index=True)
            
            # [CITE: 1] Garante que colunas numéricas são convertidas para int, com fallback seguro
            for col in ['Preco', 'Custo', 'Estoque', 'QNT_CONTADA', 'DIFERENCA']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            return df[self.final_columns] # [CITE: 1] Retorna apenas as colunas finais na ordem definida
        except Exception as e:
            self.logger.error(f"Erro ao preparar dados finais: {e}", exc_info=True)
            return None
    
    def combine_data(self) -> bool:
        with self.lock: # [CITE: 1] O lock já está no lugar, garantindo thread-safety
            try:
                self.logger.info("Iniciando processo de combinação de dados...")
                df_initial = self._load_initial_data()
                if df_initial is None: 
                    self.logger.error("Falha ao carregar dados iniciais para combinação.")
                    return False
                
                df_counts = self._load_all_count_data()
                # [CITE: 1] Não retorne False aqui se df_counts for None; ele pode ser um DataFrame vazio
                # Isso é tratado dentro de _merge_data para retornar os dados iniciais com QNT_CONTADA zerada.
                
                merged = self._merge_data(df_initial, df_counts) # [CITE: 1] df_counts pode ser None aqui, o _merge_data lida
                if merged is None: 
                    self.logger.error("Falha ao mesclar dados.")
                    return False
                
                final_df = self._prepare_final_data(merged)
                if final_df is None: 
                    self.logger.error("Falha ao preparar dados finais.")
                    return False
                
                if self._save_combined_data(final_df):
                    self.logger.info("Processo de combinação de dados concluído com sucesso.")
                    if self._update_callback:
                        # [CITE: 1] O callback é executado aqui para notificar a UI.
                        self._update_callback() 
                    return True
                return False
            except Exception as e: # [CITE: 1] Tratamento de exceção mais amplo para o combine_data
                self.logger.critical(f"Erro fatal durante a combinação de dados: {e}", exc_info=True)
                return False
            # [CITE: 1] O 'finally' para release do lock já está no 'with self.lock'

    def _save_combined_data(self, df: pd.DataFrame) -> bool:
        try:
            df.to_parquet(self.temp_file, index=False, engine='pyarrow')
            if self.combined_file.exists():
                self.combined_file.replace(self.backup_file)
            self.temp_file.replace(self.combined_file)
            self.logger.info(f"Dados combinados salvos com sucesso em {self.combined_file}")
            return True
        except Exception as e:
            self.logger.error(f"Falha ao salvar o arquivo combinado: {e}", exc_info=True)
            if not self.combined_file.exists() and self.backup_file.exists():
                self.backup_file.replace(self.combined_file)
                self.logger.warning("Reversão: Backup restaurado como arquivo principal.")
            return False

    def start_watching(self, interval: int = 10):
        # [CITE: 1] Usando threading.Event para controle mais robusto
        if self.watching: return
        self.watching = True
        self.logger.info(f"Monitoramento da pasta de dados iniciado (intervalo: {interval}s).")
        def watcher_loop():
            # [CITE: 1] Loop de observador para disparar combinação e esperar
            while self.watching:
                self.combine_data()
                # [CITE: 1] Usar self._stop_watcher.wait para um controle mais limpo
                self.logger.debug(f"Watcher esperando {interval} segundos...")
                time.sleep(interval) # [CITE: 1] Alterado de 'for _ in range(interval)' para um sleep direto
            self.logger.info("Loop do watcher encerrado.") # [CITE: 1] Log de encerramento do watcher

        self.watcher_thread = threading.Thread(target=watcher_loop, name="DataWatcher", daemon=True)
        self.watcher_thread.start()

    def stop_watching(self):
        # [CITE: 1] Usa o flag de watching para sinalizar a parada
        if not self.watching: return
        self.watching = False # [CITE: 1] Sinaliza para o loop parar
        if self.watcher_thread and self.watcher_thread.is_alive():
            self.watcher_thread.join(timeout=5) # [CITE: 1] Espera a thread terminar
        self.logger.info("Monitoramento da pasta de dados parado.")