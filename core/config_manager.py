#config_manager.py
import json
import os
from pathlib import Path
from typing import Dict, Any
import logging

class ConfigManager:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Carrega o arquivo de configuração ou cria um novo"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            else:
                return self._create_default_config()
        except Exception as e:
            self.logger.error(f"Erro ao carregar configuração: {e}")
            return self._create_default_config()
    
    def _create_default_config(self) -> Dict[str, Any]:
        """Cria configuração padrão"""
        return {
            "ui": {
                "theme": "clam",
                "font": "Arial 10",
                "max_rows": 1000,
                "row_step": 100
            },
            "colors": {
                "positive_diff": "#ffdddd",
                "negative_diff": "#ffffcc",
                "text_positive": "#000000",
                "text_negative": "#000000"
            },
            "watcher": {
                "enabled": True,
                "interval": 60
            }
        }
    
    def save_config(self):
        """Salva a configuração atual no arquivo"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
        except Exception as e:
            self.logger.error(f"Erro ao salvar configuração: {e}")
    
    def get(self, key: str, default=None) -> Any:
        """Obtém um valor de configuração"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            value = value.get(k, {})
            if not value:
                return default
        return value if value != {} else default
    
    def set(self, key: str, value: Any):
        """Define um valor de configuração"""
        keys = key.split('.')
        config = self.config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
        self.save_config()