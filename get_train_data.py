import datetime
import json
import sys
import time
import random
from itertools import zip_longest
from bs4 import BeautifulSoup
import undetected_chromedriver as uc

from get_delays import get_delays
from logger_config import setup_logging

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
]

MAX_RETRIES = 4


def get_train_data(date: str, logger) -> list:
    logger.info(f"Rozpoczęto pobieranie danych o pociągach na dzień: {date}")
    data = []
    page_num = 1

    opts = uc.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=pl-PL")

    # rotacja UA w Chrome
    opts.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

    driver = uc.Chrome(options=opts)

    try:
        while True:
            url = (
                f"https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html"
                f"?location=&date={date}&category%5Beic_premium%5D=eip&category%5Beic%5D=eic"
                f"&category%5Bic%5D=ic&category%5Btlk%5D=tlk&page={page_num}"
            )
            logger.info(f"Pobieranie strony {page_num}: {url}")

            attempt = 0
            while attempt < MAX_RETRIES:
                attempt += 1
                try:
                    driver.get(url)
                    html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")

                    table = soup.find("table")
                    if not table:
                        logger.info(f"Nie znaleziono tabeli na stronie {page_num}. Koniec danych.")
                        return _flatten_data(data, date, logger)

                    rows = table.find_all("tr")[1:]
                    if not rows:
                        logger.info(f"Tabela na stronie {page_num} jest pusta. Koniec danych.")
                        return _flatten_data(data, date, logger)

                    page_data = [
                        [td.get_text(strip=True) for td in row.find_all("td")[:5] + row.find_all("td")[6:]]
                        for row in rows
                    ]
                    data.append(page_data)
                    page_num += 1
                    time.sleep(random.uniform(0.4, 1.2))
                    break
                except Exception as e:
                    backoff = (2 ** attempt) + random.uniform(0.5, 2.0)
                    logger.warning(
                        f"Błąd przy pobieraniu strony (próba {attempt}/{MAX_RETRIES}): {e}. Odsypiam {backoff:.1f}s")
                    time.sleep(backoff)
                    # zmień UA
                    opts.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
            else:
                logger.error("Nie udało się pobrać strony po kilku próbach.")
                return _flatten_data(data, date, logger)
    finally:
        driver.quit()


def _flatten_data(data_pages, date, logger):
    headers = ["domestic", "number", "category", "name", "from", "to", "occupancy", "delay_info", "date"]
    res = []
    for page in data_pages:
        for train in page:
            train_dict = dict(zip_longest(headers, train, fillvalue="n/a"))
            train_dict["date"] = date
            res.append(train_dict)
    logger.info(f"Pobrano podstawowe dane dla {len(res)} pociągów.")
    return res


if __name__ == "__main__":
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info("ROZPOCZĘTO NOWY PROCES SCRAPOWANIA DANYCH O POCIĄGACH")
    logger.info("=" * 50)

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    train_data_wo_delays = get_train_data(today, logger)

    if not train_data_wo_delays:
        logger.warning("Nie udało się pobrać żadnych danych o pociągach. Zamykanie aplikacji.")
        sys.exit(0)

    logger.info("Rozpoczęto proces pobierania informacji o opóźnieniach...")
    data_with_delays = get_delays(train_data_wo_delays, logger)

    now_str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
    output_filename = f"train_data_{now_str}.json"
    logger.info(f"Zapisywanie wszystkich danych do pliku: {output_filename}")

    try:
        with open(output_filename, "w", encoding="utf-8-sig") as f:
            json.dump(data_with_delays, f, ensure_ascii=False, indent=4)
        logger.info("Zapis danych zakończony pomyślnie.")
    except IOError as e:
        logger.critical(f"Nie udało się zapisać pliku JSON: {e}")

    logger.info("=" * 50)
    logger.info("PROCES SCRAPOWANIA ZAKOŃCZONY")
    logger.info("=" * 50)
