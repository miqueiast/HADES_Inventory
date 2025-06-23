# core/api_collector.py
import requests
import pandas as pd
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Callable
import threading

# --- Configurações (estas podem ser globais se a classe as acessa) ---
API_BASE_URL = "https://api-minipreco-inventario-hades.onrender.com"
# MUITO IMPORTANTE: Verifique se este TOKEN está COMPLETO e CORRETO.
# Ele deve ser um token JWT válido, com 3 segmentos separados por pontos.
# Se você o obteve da API, certifique-se de que não foi cortado ou alterado.
TOKEN_EXCLUSIVO_GET = "FDAGHFH$@&@#$&$#%YFHGBSZDGHBSDFHADFHSGHSDFJFJSDFJXCVBQDFG$@&¨¨&#*(&GET12345!" 

FETCH_INTERVAL_SECONDS = 60  
API_FETCH_RETRIES = 3
API_FETCH_RETRY_DELAY_SECONDS = 5
API_REQUEST_TIMEOUT_SECONDS = 120  

# Mapeamento de colunas (estas podem ser globais se a classe as acessa)
COLUMN_MAPPING = {
    'loja_key': 'LOJA_KEY',
    'tag_operador': 'OPERADOR',
    'tag_endereco': 'ENDERECO',
    'codigo_produto': 'COD_BARRAS',
    'quantidade': 'QNT_CONTADA',
    'recontagem': 'RECONTAGEM',
    'data_contagem': 'DATA_CONTAGEM',
    'horario_recebimento_api': 'HORARIO_RECEBIMENTO_API'
}

class ApiCollector:
    """
    Classe para coletar dados de inventário de uma API em segundo plano,
    filtrar pelo inventário ativo e salvar localmente.
    """
    def __init__(self, inventory_data_path: Path, api_token: str, loja_key: str, update_callback: Optional[Callable[[], None]] = None):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.inventory_data_path = inventory_data_path
        self.api_token = api_token # Este é o token passado da MainWindow
        self.loja_key = int(loja_key) 
        self.update_callback = update_callback

        self.is_running = False
        self.thread = None
        
        # --- Configurações (referencie as globais ou defina localmente) ---
        self.api_base_url = API_BASE_URL # Use a global
        self.fetch_interval_seconds = FETCH_INTERVAL_SECONDS # Use a global
        self.api_fetch_retries = API_FETCH_RETRIES # Use a global
        self.api_fetch_retry_delay_seconds = API_FETCH_RETRY_DELAY_SECONDS # Use a global
        
        # --- Arquivos ---
        self.output_file = self.inventory_data_path / "api_counts.parquet"
        self.timestamp_state_file = self.inventory_data_path / ".last_fetched_api_timestamp.txt"
        
        # --- Mapeamento de Colunas ---
        self.column_mapping = COLUMN_MAPPING # Use a global

    def start(self):
        """Inicia o loop de coleta de dados em uma thread separada."""
        if self.is_running:
            self.logger.warning("Coletor da API já está em execução.")
            return
        self.is_running = True
        self.thread = threading.Thread(target=self._collector_loop, daemon=True, name="ApiCollectorThread")
        self.thread.start()
        self.logger.info(f"Coletor da API iniciado para a loja {self.loja_key} no inventário: {self.inventory_data_path}")

    def stop(self):
        """Para o loop de coleta de dados."""
        self.is_running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        self.logger.info("Coletor da API parado.")

    def _load_last_timestamp(self) -> Optional[datetime]:
        """Carrega o timestamp do último dado processado."""
        try:
            if self.timestamp_state_file.exists():
                ts_str = self.timestamp_state_file.read_text(encoding='utf-8').strip()
                self.logger.info(f"Timestamp da API carregado: {ts_str}")
                return datetime.fromisoformat(ts_str).astimezone(timezone.utc)
            return None
        except Exception as e:
            self.logger.error(f"Erro ao carregar timestamp da API: {e}", exc_info=True)
            return None

    def _save_last_timestamp(self, timestamp: datetime):
        """Salva o timestamp do último dado processado."""
        try:
            ts_iso = timestamp.astimezone(timezone.utc).isoformat()
            self.timestamp_state_file.write_text(ts_iso, encoding='utf-8')
            self.logger.info(f"Timestamp da API salvo: {ts_iso}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar timestamp da API: {e}", exc_info=True)

    def _fetch_data_from_api(self) -> Optional[List[Dict[str, Any]]]:
        """Busca TODOS os dados da API para filtragem local."""
        
        # A API é GET. Endpoint e método devem ser verificados com a documentação REAL da API.
        # Use o endpoint que você CONFIRMOU ser o correto para GET.
        # Pelo seu log recente, '/ver-dados/' é o endpoint que está sendo usado.
        endpoint = f"{self.api_base_url.rstrip('/')}/ver-dados/" 
        
        # O cabeçalho Authorization com 'Bearer TOKEN' está correto.
        # O problema do 401 "Not enough segments" é quase certeza que o 'self.api_token' está malformado.
        headers = {
            "Authorization": f"Bearer {self.api_token}", 
            "accept": "application/json"
        }
        
        for attempt in range(self.api_fetch_retries):
            try:
                response = requests.get(endpoint, headers=headers, timeout=API_REQUEST_TIMEOUT_SECONDS) # Mantenha o GET
                
                if response.status_code == 200:
                    api_response = response.json()
                    # Trata resposta da API: pode ser 'dados' dentro de um dict ou uma lista direta
                    if isinstance(api_response, dict) and 'dados' in api_response:
                        data = api_response['dados']
                    elif isinstance(api_response, list):
                        data = api_response
                    else:
                        self.logger.error(f"Formato de resposta inesperado da API: {type(api_response)}. Resposta: {response.text[:200]}")
                        return None

                    self.logger.info(f"API retornou {len(data)} registros no total (antes da filtragem).")
                    return data
                self.logger.warning(f"Falha na chamada da API (Status: {response.status_code}), tentativa {attempt + 1}. Resposta: {response.text}")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Erro de requisição (tentativa {attempt + 1}): {e}")
            if attempt < self.api_fetch_retries - 1:
                time.sleep(self.api_fetch_retry_delay_seconds)
        return None

    def _process_and_save_data(self, api_records: List[Dict[str, Any]], last_processed_ts: Optional[datetime]):
        """
        Filtra os dados pela loja e por novos timestamps, e salva localmente no arquivo parquet,
        substituindo contagens antigas para o mesmo item/loja se houverem.
        """
        if not api_records:
            self.logger.info("Nenhum registro da API para processar neste ciclo.")
            return last_processed_ts

        new_records_from_api = []
        current_batch_latest_ts = last_processed_ts

        for record in api_records:
            try:
                ts_str = record.get('horario_recebimento_api') or record.get('horario')
                if not ts_str:
                    self.logger.warning(f"Registro da API sem timestamp, ignorado: {record}")
                    continue

                record_ts = datetime.fromisoformat(ts_str).astimezone(timezone.utc)
                
                # 1. Filtra pela loja_key correta (esta lógica permanece)
                if record.get('loja_key') != self.loja_key:
                    continue 

                # 2. Filtra por registros que ainda não foram processados (mais novos que o último timestamp salvo)
                # Esta parte garante que processamos apenas dados que são realmente 'novos' em termos de timestamp da API.
                if last_processed_ts is None or record_ts > last_processed_ts:
                    new_records_from_api.append(record)
                    # Atualiza o timestamp mais recente encontrado NESTE LOTE de novos registros
                    if current_batch_latest_ts is None or record_ts > current_batch_latest_ts:
                        current_batch_latest_ts = record_ts
            except Exception as e:
                self.logger.warning(f"Ignorando registro da API por erro de processamento (timestamp ou loja_key): {record}. Erro: {e}")

        if not new_records_from_api:
            self.logger.info(f"Nenhum registro NOVO da API encontrado para a loja {self.loja_key} desde a última coleta.")
            return last_processed_ts

        self.logger.info(f"Processando {len(new_records_from_api)} registros NOVOS da API para a loja {self.loja_key}.")
        df_new_api_data = pd.DataFrame(new_records_from_api).rename(columns=self.column_mapping)
        
        # Garante que todas as colunas mapeadas existam no DataFrame recém-criado
        for col in self.column_mapping.values():
            if col not in df_new_api_data.columns:
                df_new_api_data[col] = '' # Ou 0 para QNT_CONTADA, dependendo do default
        
        try:
            df_existing = pd.DataFrame()
            if self.output_file.exists():
                df_existing = pd.read_parquet(self.output_file)
                self.logger.info(f"Carregado {len(df_existing)} registros existentes de {self.output_file.name}.")
            
            # --- LÓGICA CHAVE PARA ATUALIZAÇÃO E SUBSTITUIÇÃO ---
            # 1. Concatena os dados existentes com os novos.
            df_temp_combined = pd.concat([df_existing, df_new_api_data], ignore_index=True)
            
            # 2. Ordena pelos campos de identificação do item e pelo timestamp de recebimento da API (mais recente por último).
            # É CRÍTICO que 'horario_recebimento_api' seja o nome da coluna original ou mapeada que contém o timestamp.
            # Se a coluna com o timestamp real após o mapeamento for diferente, ajuste 'HORARIO_RECEBIMENTO_API'.
            
            # Garante que 'HORARIO_RECEBIMENTO_API' existe e é datetime para ordenação.
            sort_cols_for_dedup = []
            if 'COD_BARRAS' in df_temp_combined.columns:
                sort_cols_for_dedup.append('COD_BARRAS')
            if 'LOJA_KEY' in df_temp_combined.columns: # Adicione LOJA_KEY se for parte da chave única do item
                sort_cols_for_dedup.append('LOJA_KEY')
            
            if 'HORARIO_RECEBIMENTO_API' in df_temp_combined.columns:
                df_temp_combined['HORARIO_RECEBIMENTO_API'] = pd.to_datetime(df_temp_combined['HORARIO_RECEBIMENTO_API'], errors='coerce', utc=True)
                sort_cols_for_dedup.append('HORARIO_RECEBIMENTO_API')
            else:
                self.logger.warning("Coluna 'HORARIO_RECEBIMENTO_API' não encontrada para ordenação de deduplicação. "
                                    "A deduplicação manterá a última ocorrência com base na ordem de concatenação se timestamps forem iguais.")
                # Se não há timestamp, a "novidade" é determinada pela ordem de concatenação (novos vêm depois)

            # Só ordena se houver colunas para ordenar
            if sort_cols_for_dedup:
                df_temp_combined.sort_values(by=sort_cols_for_dedup, ascending=True, inplace=True, na_position='first')
            
            # 3. Remove duplicatas, mantendo a ÚLTIMA (que é a mais recente devido à ordenação).
            # As chaves 'subset' devem identificar UNICAMENTE um item de inventário.
            # 'COD_BARRAS' e 'LOJA_KEY' juntos são uma boa chave composta.
            deduplication_subset = ['COD_BARRAS', 'LOJA_KEY']
            # Garante que as colunas do subset realmente existem no DataFrame antes de usar
            deduplication_subset = [col for col in deduplication_subset if col in df_temp_combined.columns]

            if not deduplication_subset:
                self.logger.error("Não foi possível encontrar colunas para a chave de deduplicação (COD_BARRAS, LOJA_KEY). "
                                   "Os dados podem não ser atualizados corretamente no Parquet.")
                # Em um cenário crítico, você pode levantar um erro ou retornar False aqui.
                # Por enquanto, prosseguimos com concatenação simples se não houver subset.
                deduplicated_df = df_temp_combined
            else:
                deduplicated_df = df_temp_combined.drop_duplicates(subset=deduplication_subset, keep='last')
            
            self.logger.info(f"Dados do API_COUNTS.PARQUET atualizados. Total de {len(deduplicated_df)} registros únicos após deduplicação.")
            
            deduplicated_df.to_parquet(self.output_file, index=False)
            
            if self.update_callback:
                self.logger.info("Disparando callback de atualização para a interface.")
                self.update_callback()

            return current_batch_latest_ts # Retorna o timestamp do registro mais recente processado NESTE LOTE
        except Exception as e:
            self.logger.error(f"Erro ao salvar e deduplicar dados da API em {self.output_file}: {e}", exc_info=True)
            return last_processed_ts

    def _collector_loop(self):
        """Loop principal que busca e processa dados periodicamente."""
        last_timestamp = self._load_last_timestamp()
        
        while self.is_running:
            self.logger.info(f"--- [Loja {self.loja_key}] Iniciando ciclo de busca da API ---")
            api_data = self._fetch_data_from_api()
            
            if api_data is not None:
                new_latest_timestamp = self._process_and_save_data(api_data, last_timestamp)
                if new_latest_timestamp and (last_timestamp is None or new_latest_timestamp > last_timestamp):
                    last_timestamp = new_latest_timestamp
                    self._save_last_timestamp(last_timestamp)
            
            # Aguarda o próximo ciclo
            for _ in range(self.fetch_interval_seconds):
                if not self.is_running: 
                    break
                time.sleep(1)