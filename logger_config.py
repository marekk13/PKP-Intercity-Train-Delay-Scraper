import logging
import os
import sys
from datetime import datetime


def setup_logging():
    """
    Konfiguruje logowanie do konsoli i do pliku.

    Ustawia dwa handlery: jeden dla konsoli (StreamHandler) i jeden dla pliku (FileHandler).
    """
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.INFO)

    output_dir = "logs"
    os.makedirs(output_dir, exist_ok=True)
    log_filename = os.path.join(output_dir, f"scraper_log_{datetime.now().strftime('%Y-%m-%d-%H%M')}.log")
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)  # Logi od poziomu INFO i wyższe trafią do pliku

    # wyświetlanie logów w konsoli
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    stream_handler.setLevel(logging.INFO)  # logi od poziomu INFO i wyższe trafią do konsoli

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger