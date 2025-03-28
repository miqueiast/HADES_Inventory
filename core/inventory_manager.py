import os
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
import shutil
import logging

class InventoryManager:
    def __init__(self, data_folder: str = "data"):
        """
        Gerenciador de inventários que controla a criação, seleção e acesso aos dados dos inventários.
        
        Args:
            data_folder (str): Pasta principal onde os inventários serão armazenados. Padrão: "data".
        """
        self.active_inventory = None
        self.active_inventory_path = None
        self.data_folder = data_folder
        self.logger = logging.getLogger(__name__)
        
        # Cria pasta principal se não existir
        os.makedirs(self.data_folder, exist_ok=True)
    
    def create_inventory(self, inventory_name: str, store: str) -> Optional[Dict[str, Any]]:
        """
        Cria um novo inventário e define como ativo automaticamente.
        
        Args:
            inventory_name (str): Nome do inventário a ser criado
            store (str): Nome da loja associada ao inventário
            
        Returns:
            Dict[str, Any]: Dicionário com informações do inventário criado ou None em caso de erro
        """
        try:
            # Remove caracteres inválidos do nome
            safe_name = "".join(c for c in inventory_name if c.isalnum() or c in (' ', '_')).rstrip()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            folder_name = f"{safe_name}_{store}_{timestamp}"
            inventory_path = os.path.join(self.data_folder, folder_name)
            
            if os.path.exists(inventory_path):
                self.logger.warning(f"Inventário já existe: {inventory_path}")
                return None
                
            # Cria estrutura de pastas
            os.makedirs(inventory_path)
            os.makedirs(os.path.join(inventory_path, "dados"))
            
            # Cria metadados
            metadata = {
                "nome": inventory_name,
                "loja": store,
                "criado_em": datetime.now().isoformat(),
                "ultima_modificacao": datetime.now().isoformat(),
                "status": "ativo"
            }
            
            # Salva metadados
            pd.DataFrame([metadata]).to_parquet(os.path.join(inventory_path, "metadata.parquet"))
            
            # Define como ativo automaticamente
            self.active_inventory = inventory_name
            self.active_inventory_path = inventory_path
            
            self.logger.info(f"Inventário criado e ativado: {inventory_name}")
            return {
                "name": inventory_name,
                "store": store,
                "path": inventory_path,
                "created_at": metadata["criado_em"]
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao criar inventário: {e}", exc_info=True)
            return None
    
    def set_active_inventory(self, inventory_path: str) -> bool:
        """
        Define um inventário existente como ativo.
        
        Args:
            inventory_path (str): Caminho completo para o inventário
            
        Returns:
            bool: True se o inventário foi ativado com sucesso, False caso contrário
        """
        try:
            if not os.path.exists(inventory_path):
                self.logger.warning(f"Caminho não existe: {inventory_path}")
                return False
                
            metadata_path = os.path.join(inventory_path, "metadata.parquet")
            if not os.path.exists(metadata_path):
                self.logger.warning(f"Metadados não encontrados em: {inventory_path}")
                return False
                
            # Atualiza metadados
            metadata = pd.read_parquet(metadata_path).iloc[0].to_dict()
            metadata["ultima_modificacao"] = datetime.now().isoformat()
            metadata["status"] = "ativo"
            pd.DataFrame([metadata]).to_parquet(metadata_path)
            
            # Define como ativo
            self.active_inventory = metadata.get("nome", "Inventário Desconhecido")
            self.active_inventory_path = inventory_path
            
            self.logger.info(f"Inventário ativado: {self.active_inventory}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao ativar inventário: {e}", exc_info=True)
            return False
    
    def get_inventory_list(self) -> List[Dict[str, Any]]:
        """
        Retorna lista detalhada de todos os inventários disponíveis.
        
        Returns:
            List[Dict[str, Any]]: Lista de dicionários com informações de cada inventário
        """
        inventories = []
        try:
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
                                "created_at": metadata.get("criado_em", ""),
                                "status": metadata.get("status", "inativo")
                            })
                        except Exception:
                            continue
        except Exception as e:
            self.logger.error(f"Erro ao listar inventários: {e}", exc_info=True)
            
        return sorted(inventories, key=lambda x: x.get("created_at", ""), reverse=True)
    
    def get_active_inventory_data_path(self) -> Optional[str]:
        """
        Retorna o caminho para a pasta de dados do inventário ativo.
        
        Returns:
            Optional[str]: Caminho para a pasta de dados ou None se não houver inventário ativo
        """
        if self.active_inventory_path:
            data_path = os.path.join(self.active_inventory_path, "dados")
            if os.path.exists(data_path):
                return data_path
        return None
    
    def get_active_inventory_info(self) -> Optional[Dict[str, Any]]:
        """
        Retorna informações completas do inventário ativo.
        
        Returns:
            Optional[Dict[str, Any]]: Dicionário com metadados do inventário ou None se não houver inventário ativo
        """
        if not self.active_inventory_path:
            return None
            
        metadata_path = os.path.join(self.active_inventory_path, "metadata.parquet")
        if os.path.exists(metadata_path):
            try:
                return pd.read_parquet(metadata_path).iloc[0].to_dict()
            except Exception:
                pass
        return None
    
    def get_active_inventory_combined_data(self) -> Optional[pd.DataFrame]:
        """
        Retorna os dados combinados do inventário ativo como um DataFrame.
        
        Returns:
            Optional[pd.DataFrame]: DataFrame com os dados combinados ou None se não houver dados ou inventário ativo
        """
        if not self.active_inventory_path:
            self.logger.debug("Nenhum inventário ativo para obter dados combinados")
            return None
            
        data_path = self.get_active_inventory_data_path()
        if not data_path:
            self.logger.debug("Nenhum caminho de dados encontrado para o inventário ativo")
            return None
            
        combined_file = Path(data_path) / "combined_data.parquet"
        if combined_file.exists():
            try:
                self.logger.info(f"Carregando dados combinados de {combined_file}")
                return pd.read_parquet(combined_file)
            except Exception as e:
                self.logger.error(f"Erro ao ler arquivo combinado: {e}", exc_info=True)
        else:
            self.logger.debug(f"Arquivo combinado não encontrado: {combined_file}")
        
        return None
    
    def delete_inventory(self, inventory_path: str) -> bool:
        """
        Remove completamente um inventário.
        
        Args:
            inventory_path (str): Caminho completo para o inventário a ser removido
            
        Returns:
            bool: True se o inventário foi removido com sucesso, False caso contrário
        """
        try:
            if not os.path.exists(inventory_path):
                self.logger.warning(f"Caminho não existe para exclusão: {inventory_path}")
                return False
                
            # Se estiver deletando o inventário ativo, desativa primeiro
            if self.active_inventory_path == inventory_path:
                self.active_inventory = None
                self.active_inventory_path = None
                
            shutil.rmtree(inventory_path)
            self.logger.info(f"Inventário removido: {inventory_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao remover inventário: {e}", exc_info=True)
            return False
    
    def backup_inventory(self, inventory_path: str, backup_dir: str = "backups") -> Optional[str]:
        """
        Cria um backup compactado do inventário.
        
        Args:
            inventory_path (str): Caminho completo para o inventário
            backup_dir (str): Pasta onde o backup será armazenado (relativa à pasta data)
            
        Returns:
            Optional[str]: Caminho para o arquivo de backup criado ou None em caso de erro
        """
        try:
            if not os.path.exists(inventory_path):
                self.logger.warning(f"Caminho não existe para backup: {inventory_path}")
                return None
                
            # Cria pasta de backups se não existir
            full_backup_dir = os.path.join(self.data_folder, backup_dir)
            os.makedirs(full_backup_dir, exist_ok=True)
            
            # Nome do arquivo de backup
            inventory_name = os.path.basename(inventory_path)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(full_backup_dir, f"{inventory_name}_{timestamp}.zip")
            
            # Cria o arquivo zip
            shutil.make_archive(backup_file.replace('.zip', ''), 'zip', inventory_path)
            
            self.logger.info(f"Backup criado: {backup_file}")
            return backup_file
            
        except Exception as e:
            self.logger.error(f"Erro ao criar backup: {e}", exc_info=True)
            return None