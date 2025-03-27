# inventory_manager.py
import os
import json
from datetime import datetime

class InventoryManager:
    def __init__(self):
        self.inventories_root = os.path.join(os.getcwd(), "Inventário")
        os.makedirs(self.inventories_root, exist_ok=True)
    
    def create_inventory(self, name, store, creator):
        """Cria um novo inventário com estrutura de pastas"""
        # Cria nome da pasta (substitui espaços por underscores)
        folder_name = f"{name.replace(' ', '_')}_{store.replace(' ', '_')}"
        folder_path = os.path.join(self.inventories_root, folder_name)
        
        # Verifica se já existe
        if os.path.exists(folder_path):
            raise Exception(f"Já existe um inventário com o nome '{name}' para a loja '{store}'")
        
        # Cria estrutura de pastas
        os.makedirs(folder_path)
        dados_path = os.path.join(folder_path, "dados")
        os.makedirs(dados_path)
        
        # Cria arquivo com informações do criador
        creator_info = {
            'creator': creator,
            'created_at': datetime.now().isoformat()
        }
        with open(os.path.join(folder_path, "creator_info.json"), 'w') as f:
            json.dump(creator_info, f, indent=2)
        
        # Cria metadados do inventário
        inventory = {
            'name': name,
            'store': store,
            'creator': creator,
            'path': folder_path,
            'created_at': datetime.now().isoformat(),
            'last_modified': datetime.now().isoformat()
        }
        
        # Salva metadados
        with open(os.path.join(folder_path, "metadata.json"), 'w') as f:
            json.dump(inventory, f, indent=2)
        
        return inventory
    
    def load_inventory(self, folder_path):
        """Carrega um inventário existente"""
        metadata_path = os.path.join(folder_path, "metadata.json")
        
        if not os.path.exists(metadata_path):
            raise Exception("Pasta selecionada não contém um inventário válido")
        
        with open(metadata_path, 'r') as f:
            inventory = json.load(f)
        
        return inventory
    
    def save_inventory(self, inventory):
        """Salva os metadados do inventário"""
        inventory['last_modified'] = datetime.now().isoformat()
        metadata_path = os.path.join(inventory['path'], "metadata.json")
        
        with open(metadata_path, 'w') as f:
            json.dump(inventory, f, indent=2)
    
    def list_inventories(self):
        """Lista todos os inventários disponíveis"""
        return [d for d in os.listdir(self.inventories_root) 
                if os.path.isdir(os.path.join(self.inventories_root, d))]