# zamień zawartość funkcji get_train_data na to
import datetime
import json
import sys
import time
import random
from itertools import zip_longest
from requests_html import HTMLSession
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.remote_connection import RemoteConnection

from get_delays import get_delays
from logger_config import setup_logging

USER_AGENTS = [
    # krótka lista, możesz dorzucić więcej
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15"
]

def get_train_data(date: str, logger) -> list:
    logger.info(f"Rozpoczęto pobieranie podstawowych danych o pociągach na dzień: {date}")

    data = []
    i = 1

    session = HTMLSession()

    # bazowe nagłówki "ludzkie"
    base_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1",
    }

    max_retries = 4

    while True:
        try:
            url = f"https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html?location=&date={date}&category%5Beic_premium%5D=eip&category%5Beic%5D=eic&category%5Bic%5D=ic&category%5Btlk%5D=tlk&page={i}"
            logger.info(f"Pobieranie danych ze strony {i}: {url}")

            # rotuj UA
            headers = base_headers.copy()
            headers["User-Agent"] = random.choice(USER_AGENTS)

            attempt = 0
            while attempt < max_retries:
                attempt += 1
                try:
                    response = session.get(url, headers=headers, timeout=30)
                    status = response.status_code
                    if status == 200:
                        break
                    elif status == 403:
                        backoff = (2 ** attempt) + random.uniform(0.5, 2.0)
                        logger.warning(f"403 z serwera (próba {attempt}/{max_retries}). Odsypiam {backoff:.1f}s")
                        time.sleep(backoff)
                        headers["User-Agent"] = random.choice(USER_AGENTS)
                        continue
                    else:
                        response.raise_for_status()
                except Exception as e:
                    logger.warning(f"Błąd sieciowy przy pobieraniu strony {i}, próba {attempt}: {e}")
                    time.sleep(1 + random.uniform(0, 1.5))
            else:
                # wszystkie retry nieudane -> spróbuj fallback na Selenium (jeśli dostępny)
                logger.warning("Wszystkie próby HTTP nieudane — próbuję fallback: Selenium (jeśli dostępny).")
                try:
                    # Próba pobrania przez Selenium Remote (jeśli masz serwer Selenium)
                    opts = Options()
                    opts.add_argument("--headless=new")
                    opts.add_argument("--no-sandbox")
                    opts.add_argument("--disable-dev-shm-usage")
                    opts.add_argument("--lang=pl-PL")
                    driver = webdriver.Remote(command_executor='http://localhost:4444/wd/hub', options=opts)
                    driver.get(url)
                    page_source = driver.page_source
                    driver.quit()
                    html = session.html_class(page_source)  # requests_html HTML z surowym source
                except Exception as se:
                    logger.error(f"Fallback Selenium również nie powiódł się: {se}")
                    raise RuntimeError("Nie udało się pobrać strony intercity (403 i Selenium fallback nieudany).")
            # jeśli response istnieje (status 200) -> użyj response.html
            if 'response' in locals() and response.status_code == 200:
                html = response.html

            table = html.find("table", first=True)

            if not table:
                logger.info(f"Na stronie {i} nie znaleziono tabeli z danymi. Prawdopodobnie to koniec wyników.")
                break

            page_data = [
                [d.text for d in row.find("td")[:5] + row.find("td")[6:]]
                for row in table.find("tr")[1:]
            ]

            if not page_data:
                logger.info(f"Tabela na stronie {i} jest pusta. Zakończono pobieranie.")
                break

            data.append(page_data)
            i += 1

            # small human-like sleep between page requests
            time.sleep(random.uniform(0.4, 1.1))
        except Exception as e:
            logger.error(f"Wystąpił błąd na stronie {i}: {e}")
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

    logger.info(f"Pobrano podstawowe dane dla {len(res)} pociągów.")
    return res
