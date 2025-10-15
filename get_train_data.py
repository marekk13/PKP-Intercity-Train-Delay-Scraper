import datetime
import json
import sys
import time
import random
import traceback
from itertools import zip_longest
from requests_html import HTMLSession
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

from get_delays import get_delays
from logger_config import setup_logging

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def get_train_data(date: str, logger) -> list:
    logger.info(f"Rozpoczęto pobieranie podstawowych danych o pociągach na dzień: {date}")

    data = []
    i = 1
    session = HTMLSession()

    base_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1",
    }

    max_retries = 4

    while True:
        url = (
            "https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html"
            f"?location=&date={date}&category%5Beic_premium%5D=eip"
            "&category%5Beic%5D=eic&category%5Bic%5D=ic&category%5Btlk%5D=tlk"
            f"&page={i}"
        )
        logger.info(f"Pobieranie danych ze strony {i}: {url}")

        headers = base_headers.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)

        response = None
        success = False

        for attempt in range(1, max_retries + 1):
            try:
                response = session.get(url, headers=headers, timeout=30)
                status = response.status_code

                if status == 200:
                    success = True
                    break

                # Obsługa 403 — często blokada botów
                elif status == 403:
                    snippet = response.text[:400].replace("\n", " ")
                    logger.warning(
                        f"403 Forbidden (próba {attempt}/{max_retries}) — UA={headers['User-Agent']}. "
                        f"Fragment odpowiedzi: {snippet!r}"
                    )
                    backoff = (2 ** attempt) + random.uniform(0.5, 2.0)
                    logger.info(f"Odsypiam {backoff:.1f}s i zmieniam User-Agent.")
                    headers["User-Agent"] = random.choice(USER_AGENTS)
                    time.sleep(backoff)
                    continue

                else:
                    logger.warning(f"HTTP {status} ({response.reason}) przy próbie {attempt}/{max_retries}")
                    snippet = response.text[:400].replace("\n", " ")
                    logger.debug(f"Odpowiedź: {snippet!r}")
                    time.sleep(random.uniform(1, 2))

            except Exception as e:
                logger.warning(f"Błąd sieciowy (próba {attempt}/{max_retries}): {e}")
                logger.debug(traceback.format_exc())
                time.sleep(1.5 + random.uniform(0, 2))

        # Jeśli wszystkie próby zawiodły — Selenium fallback
        if not success:
            logger.warning("Wszystkie próby HTTP nieudane — próbuję fallback: Selenium (jeśli dostępny).")
            try:
                opts = Options()
                opts.add_argument("--headless=new")
                opts.add_argument("--no-sandbox")
                opts.add_argument("--disable-dev-shm-usage")
                opts.add_argument("--lang=pl-PL")
                opts.add_argument("--user-agent=" + random.choice(USER_AGENTS))

                driver = webdriver.Chrome(options=opts)
                driver.get(url)
                page_source = driver.page_source
                html = session.html_class(page_source)
                driver.quit()
                logger.info("Fallback Selenium zakończony sukcesem.")
            except WebDriverException as se:
                logger.error(f"Nie udało się połączyć z Selenium Remote: {se}")
                logger.debug(traceback.format_exc())
                raise RuntimeError("Nie udało się pobrać strony Intercity — Selenium niedostępne.")
            except Exception as se:
                logger.error(f"Fallback Selenium również nie powiódł się: {se}")
                logger.debug(traceback.format_exc())
                raise RuntimeError("Nie udało się pobrać strony Intercity (403 i Selenium fallback nieudany).")
        else:
            html = response.html

        try:
            table = html.find("table", first=True)
        except Exception as e:
            logger.error(f"Nie udało się sparsować HTML na stronie {i}: {e}")
            logger.debug(traceback.format_exc())
            break

        if not table:
            logger.info(f"Na stronie {i} nie znaleziono tabeli z danymi. Prawdopodobnie koniec wyników.")
            break

        rows = table.find("tr")[1:]
        page_data = []
        for row in rows:
            cells = row.find("td")
            if not cells:
                continue
            try:
                row_data = [d.text for d in cells[:5] + cells[6:]]
                page_data.append(row_data)
            except Exception as e:
                logger.debug(f"Błąd przy parsowaniu wiersza: {e}")

        if not page_data:
            logger.info(f"Tabela na stronie {i} jest pusta. Zakończono pobieranie.")
            break

        data.append(page_data)
        logger.info(f"Pobrano {len(page_data)} rekordów ze strony {i}.")
        i += 1
        time.sleep(random.uniform(0.4, 1.1))  # mały odstęp między zapytaniami

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
        logger.info("Zapisywanie danych do pliku JSON zakończone pomyślnie.")
    except IOError as e:
        logger.critical(f"Nie udało się zapisać pliku JSON: {e}")
        logger.debug(traceback.format_exc())

    logger.info("=" * 50)
    logger.info("PROCES SCRAPOWANIA ZAKOŃCZONY")
    logger.info("=" * 50)
