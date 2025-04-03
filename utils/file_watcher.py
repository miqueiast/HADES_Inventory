#file_watcher.py
import time
import threading
from pathlib import Path
from typing import Callable, Optional
import logging

class FileWatcher:
    def __init__(self, watch_dir: str, callback: Callable, interval: int = 60):
        self.watch_dir = Path(watch_dir)
        self.callback = callback
        self.interval = interval
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger(__name__)
    
    def start(self):
        """Inicia o monitoramento do diretório"""
        if self.running:
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._watch, daemon=True)
        self.thread.start()
        self.logger.info(f"Iniciando monitoramento de {self.watch_dir}")
    
    def stop(self):
        """Para o monitoramento do diretório"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
        self.logger.info(f"Monitoramento de {self.watch_dir} parado")
    
    def _watch(self):
        """Loop principal do monitoramento"""
        last_files = set()
        
        while self.running:
            try:
                current_files = set(f.name for f in self.watch_dir.glob('*') if f.is_file())
                
                # Verifica novos arquivos
                new_files = current_files - last_files
                if new_files:
                    self.logger.info(f"Novos arquivos detectados: {new_files}")
                    self.callback(new_files)
                
                last_files = current_files
            except Exception as e:
                self.logger.error(f"Erro no monitoramento: {e}")
            
            time.sleep(self.interval)