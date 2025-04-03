import logging
import os
from pathlib import Path
from typing import Optional, Union
from logging.handlers import RotatingFileHandler

def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: Union[str, int] = logging.INFO,
    log_format: Optional[str] = None,
    max_bytes: int = 5 * 1024 * 1024,  # 5MB
    backup_count: int = 3
) -> logging.Logger:
    """
    Configura e retorna um logger avançado para o sistema HADES.
    
    Parâmetros:
        name (str): Nome do logger (normalmente o nome do módulo ou aplicação)
        log_file (str, opcional): Caminho para o arquivo de log. Se None, logs só vão para console.
        level (str/int): Nível de log (ex: 'INFO', 'DEBUG' ou logging.INFO, logging.DEBUG)
        log_format (str, opcional): Formato personalizado para as mensagens de log
        max_bytes (int): Tamanho máximo do arquivo de log antes de rotacionar (em bytes)
        backup_count (int): Número de arquivos de backup a manter
    
    Retorna:
        logging.Logger: Objeto logger configurado
    
    Exceções:
        ValueError: Se o nível de log for inválido
    """
    # Validação do nível de log
    if isinstance(level, str):
        level = level.upper()
        if not hasattr(logging, level):
            raise ValueError(f"Nível de log inválido: {level}")
        level = getattr(logging, level)
    
    # Cria ou obtém um logger com o nome especificado
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove handlers existentes para evitar duplicação
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Formato padrão das mensagens de log
    default_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format or default_format)
    
    # Handler para saída no console (terminal)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Se um arquivo de log foi especificado, configura handler com rotação
    if log_file:
        try:
            # Cria o diretório de logs se não existir
            log_dir = Path(log_file).parent
            if not log_dir.exists():
                os.makedirs(log_dir, exist_ok=True)
            
            # Cria o handler com rotação de arquivos
            file_handler = RotatingFileHandler(
                filename=log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            logger.debug(f"Handler de arquivo configurado para {log_file} "
                        f"com rotação a cada {max_bytes} bytes, "
                        f"mantendo {backup_count} backups")
            
        except Exception as e:
            logger.error(f"Falha ao configurar arquivo de log: {str(e)}")
            raise
    
    logger.debug("Logger configurado com sucesso")
    return logger