import logging
import sys
from datetime import datetime


def setup_logging():
    """
    Konfiguruje logowanie do konsoli i do pliku.

    Ustawia dwa handlery: jeden dla konsoli (StreamHandler) i jeden dla
    pliku (FileHandler). Zapewnia to, że logi są zarówno wyświetlane
    na bieżąco, jak i archiwizowane.
    """
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.INFO)  # Ustawiamy minimalny poziom logowania dla loggera

    # Handler do zapisywania logów w pliku
    log_filename = f"scraper_log_{datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)  # Logi od poziomu INFO i wyższe trafią do pliku

    # Handler do wyświetlania logów w konsoli
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    stream_handler.setLevel(logging.INFO)  # Logi od poziomu INFO i wyższe trafią do konsoli

    # Dodajemy oba handlery do głównego loggera
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger