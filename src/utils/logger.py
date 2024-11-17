import logging
import sys
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name='ImageConsumer'):
    """
    Configurează un logger cu output în consolă și fișier,
    cu rotație automată și niveluri diferite pentru consolă și fișier
    """
    # Crează directorul pentru loguri dacă nu există
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Calea completă către fișierul de log
    log_file = os.path.join(log_dir, 'consumer.log')

    # Crează logger-ul
    logger = logging.getLogger(name)
    
    # Verifică dacă logger-ul are deja handlere pentru a evita duplicarea
    if logger.handlers:
        return logger
        
    logger.setLevel(logging.DEBUG)  # Set la DEBUG pentru a permite toate nivelurile

    # Oprește propagarea pentru a evita duplicarea
    logger.propagate = False

    # Format detaliat pentru loguri
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Format simplu pentru consolă
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # Handler pentru consolă (doar INFO și erori)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Handler pentru fișier (toate mesajele, inclusiv DEBUG)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)

    # Adaugă și un handler special pentru erori
    error_file = os.path.join(log_dir, 'error.log')
    error_handler = RotatingFileHandler(
        error_file,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    logger.addHandler(error_handler)

    logger.debug("Logger initialized with console and file handlers")
    return logger