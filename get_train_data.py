import datetime
import json
import requests
import sys
from itertools import zip_longest

from requests_html import HTMLSession
from requests.models import MissingSchema

from get_delays import get_delays
from logger_config import setup_logging


def get_train_data(date: str, logger) -> list:
    """Pobiera dane o frekwencji pociągów ze strony intercity.pl."""
    logger.info(f"Rozpoczęto pobieranie podstawowych danych o pociągach na dzień: {date}")
    s = HTMLSession()
    i = 1
    data = []

    while True:
        try:
            url = f"https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html?location=&date={date}&category%5Beic_premium%5D=eip&category%5Beic%5D=eic&category%5Bic%5D=ic&category%5Btlk%5D=tlk&page={i}"
            logger.info(f"Pobieranie danych ze strony {i}: {url}")
            r = s.get(url)
            r.raise_for_status()  # Sprawdza czy status to 2xx

            table = r.html.find("table")
            if not table:
                logger.info(f"Na stronie {i} nie znaleziono tabeli z danymi. Prawdopodobnie to koniec wyników.")
                break

            table = table[0]

            page_data = [
                [d.text for d in row.find("td")[:5] + row.find("td")[6:]]
                for row in table.find("tr")[1:]
            ]

            if not page_data:
                logger.info(f"Tabela na stronie {i} jest pusta. Zakończono pobieranie.")
                break

            data.append(page_data)
            i += 1
        except IndexError:
            logger.info("Zakończono pobieranie danych z Intercity.pl - brak kolejnych stron.")
            break
        except MissingSchema:
            logger.critical(f"Nieprawidłowy URL: {url}. Zamykanie aplikacji.")
            sys.exit("Invalid URL provided (get_train_data)")
        except requests.exceptions.RequestException as e:
            logger.error(f"Błąd połączenia podczas pobierania strony {i}: {e}")
            break

    headers_data = [
        "domestic", "number", "category", "name", "from", "to",
        "occupancy", "delay_info", "date"
    ]

    res = []
    for page_index, page in enumerate(data):
        for train in page:
            train_dict = dict(zip_longest(headers_data, train, fillvalue="n/a"))
            train_dict["date"] = date
            res.append(train_dict)

    logger.info(f"Pobrano podstawowe dane dla {len(res)} pociągów.")
    return res


if __name__ == "__main__":
    # 1. Konfiguracja loggera
    logger = setup_logging()

    logger.info("=" * 50)
    logger.info("ROZPOCZĘTO NOWY PROCES SCRAPOWANIA DANYCH O POCIĄGACH")
    logger.info("=" * 50)


    # 2. Pobranie danych podstawowych
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    train_data_wo_delays = get_train_data(today, logger)

    if not train_data_wo_delays:
        logger.warning("Nie udało się pobrać żadnych danych o pociągach. Zamykanie aplikacji.")
        sys.exit(0)

    # 3. Pobranie informacji o opóźnieniach
    logger.info("Rozpoczęto proces pobierania informacji o opóźnieniach...")
    data_with_delays = get_delays(train_data_wo_delays, logger)

    # 4. Zapis wyników do pliku JSON
    now_str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
    output_filename = f"train_data_{now_str}.json"
    logger.info(f"Zapisywanie wszystkich danych do pliku: {output_filename}")

    try:
        with open(output_filename, "w", encoding="utf-8-sig") as f:
            json.dump(data_with_delays, f, ensure_ascii=False, indent=4)
        logger.info("Zapisywanie danych do pliku JSON zakończone pomyślnie.")
    except IOError as e:
        logger.critical(f"Nie udało się zapisać pliku JSON: {e}")

    logger.info("=" * 50)
    logger.info("PROCES SCRAPOWANIA ZAKOŃCZONY")
    logger.info("=" * 50)