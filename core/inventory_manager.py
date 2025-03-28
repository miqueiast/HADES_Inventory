import os
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import shutil

class InventoryManager:
    def __init__(self):
        self.active_inventory = None
        self.active_inventory_path = None
        self.data_folder = "data"
        
        # Cria pasta principal se não existir
        os.makedirs(self.data_folder, exist_ok=True)
    
    def create_inventory(self, inventory_name: str, store: str) -> bool:
        """Cria um novo inventário"""
        try:
            # Remove caracteres inválidos do nome
            safe_name = "".join(c for c in inventory_name if c.isalnum() or c in (' ', '_')).rstrip()
            folder_name = f"{safe_name}_{store}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            inventory_path = os.path.join(self.data_folder, folder_name)
            
            if os.path.exists(inventory_path):
                return False
                
            os.makedirs(inventory_path)
            os.makedirs(os.path.join(inventory_path, "dados"))
            
            # Cria arquivo de metadados básicos
            metadata = {
                "nome": inventory_name,
                "loja": store,
                "criado_em": datetime.now().isoformat(),
                "ultima_modificacao": datetime.now().isoformat()
            }
            
            pd.DataFrame([metadata]).to_parquet(os.path.join(inventory_path, "metadata.parquet"))
            
            self.active_inventory = inventory_name
            self.active_inventory_path = inventory_path
            return True
            
        except Exception as e:
            print(f"Erro ao criar inventário: {e}")
            return False
    
    def set_active_inventory(self, inventory_path: str) -> bool:
        """Define um inventário existente como ativo"""
        if os.path.exists(inventory_path) and os.path.isdir(inventory_path):
            self.active_inventory_path = inventory_path
            metadata_path = os.path.join(inventory_path, "metadata.parquet")
            
            if os.path.exists(metadata_path):
                try:
                    metadata = pd.read_parquet(metadata_path).iloc[0].to_dict()
                    self.active_inventory = metadata.get("nome", "Inventário Desconhecido")
                    return True
                except Exception:
                    pass
        return False
    
    def get_inventory_list(self) -> list:
        """Retorna lista de inventários disponíveis"""
        inventories = []
        for item in os.listdir(self.data_folder):
            full_path = os.path.join(self.data_folder, item)
            if os.path.isdir(full_path):
                metadata_path = os.path.join(full_path, "metadata.parquet")
                if os.path.exists(metadata_path):
                    try:
                        metadata = pd.read_parquet(metadata_path).iloc[0].to_dict()
                        inventories.append({
                            "path": full_path,
                            "name": metadata.get("nome", "Inventário Desconhecido"),
                            "store": metadata.get("loja", "Loja Desconhecida"),
                            "created_at": metadata.get("criado_em", "")
                        })
                    except Exception:
                        continue
        return inventories
    
    def get_active_inventory_data_path(self) -> Optional[str]:
        """Retorna o caminho para a pasta de dados do inventário ativo"""
        if self.active_inventory_path:
            return os.path.join(self.active_inventory_path, "dados")
        return None