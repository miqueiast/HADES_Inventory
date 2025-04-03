#validators.py
import re
from typing import Optional

def validate_inventory_name(name: str) -> Optional[str]:
    """Valida o nome do inventário"""
    if not name or len(name.strip()) < 3:
        return "O nome deve ter pelo menos 3 caracteres"
    if len(name) > 50:
        return "O nome deve ter no máximo 50 caracteres"
    if not re.match(r'^[\w\s\-]+$', name):
        return "Use apenas letras, números, espaços, hífens e underscores"
    return None

def validate_store_name(store: str) -> Optional[str]:
    """Valida o nome da loja"""
    if not store or len(store.strip()) < 2:
        return "O nome da loja deve ter pelo menos 2 caracteres"
    if len(store) > 30:
        return "O nome da loja deve ter no máximo 30 caracteres"
    return None

def validate_file_extension(file_path: str, valid_extensions: list) -> Optional[str]:
    """Valida a extensão do arquivo"""
    if not file_path:
        return "Nenhum arquivo selecionado"
    
    file_ext = file_path.split('.')[-1].lower()
    if file_ext not in valid_extensions:
        return f"Extensão inválida. Use: {', '.join(valid_extensions)}"
    return None