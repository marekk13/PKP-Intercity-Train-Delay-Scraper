import datetime
import json
import sys
import time
import random
from itertools import zip_longest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from get_delays import get_delays
from logger_config import setup_logging

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15"
]

def get_train_data(date: str, logger) -> list:
    logger.info(f"Rozpoczęto pobieranie danych o pociągach na dzień: {date}")
    data = []
    i = 1

    while True:
        url = f"https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html?location=&date={date}&category%5Beic_premium%5D=eip&category%5Beic%5D=eic&category%5Bic%5D=ic&category%5Btlk%5D=tlk&page={i}"
        logger.info(f"Pobieranie danych ze strony {i}: {url}")

        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--lang=pl-PL")
        opts.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

        try:
            driver = webdriver.Chrome(options=opts)
            driver.get(url)
            time.sleep(random.uniform(1, 2))  # poczekaj na pełne wyrenderowanie
            table = driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # pomijamy nagłówek
            page_data = []
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 5:
                    continue
                page_data.append([cell.text for cell in cells[:5]] + [cell.text for cell in cells[6:]])
            driver.quit()

            if not page_data:
                logger.info(f"Strona {i} nie zawiera danych – koniec paginacji.")
                break

            data.append(page_data)
            i += 1
            time.sleep(random.uniform(0.4, 1.1))

        except Exception as e:
            logger.error(f"Błąd przy pobieraniu strony {i}: {e}")
            try:
                driver.quit()
            except:
                pass
            break

    headers_data = [
        "domestic", "number", "category", "name", "from", "to",
        "occupancy", "delay_info", "date"
    ]

    res = []
    for page in data:
        for train in page:
            train_dict = dict(zip_longest(headers_data, train, fillvalue="n/a"))
            train_dict["date"] = date
            res.append(train_dict)

    logger.info(f"Pobrano dane dla {len(res)} pociągów.")
    return res

if __name__ == "__main__":
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info("ROZPOCZĘTO NOWY PROCES SCRAPOWANIA DANYCH O POCIĄGACH")
    logger.info("=" * 50)

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    train_data_wo_delays = get_train_data(today, logger)

    if not train_data_wo_delays:
        logger.warning("Nie udało się pobrać danych o pociągach. Zamykanie aplikacji.")
        sys.exit(0)

    logger.info("Rozpoczęto pobieranie informacji o opóźnieniach...")
    data_with_delays = get_delays(train_data_wo_delays, logger)

    now_str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
    output_filename = f"train_data_{now_str}.json"
    logger.info(f"Zapisywanie danych do pliku: {output_filename}")

    try:
        with open(output_filename, "w", encoding="utf-8-sig") as f:
            json.dump(data_with_delays, f, ensure_ascii=False, indent=4)
        logger.info("Zapisywanie danych zakończone pomyślnie.")
    except IOError as e:
        logger.critical(f"Nie udało się zapisać pliku JSON: {e}")

    logger.info("=" * 50)
    logger.info("PROCES SCRAPOWANIA ZAKOŃCZONY")
    logger.info("=" * 50)
