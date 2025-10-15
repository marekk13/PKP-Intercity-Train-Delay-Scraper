#!/usr/bin/env python3
# coding: utf-8
import datetime
import json
import sys
import time
import random
import os
from itertools import zip_longest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException,
    NoSuchElementException,
    TimeoutException,
)

from get_delays import get_delays
from logger_config import setup_logging

# rotacja UA
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
]

OUTPUT_DIR = "/app/outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_debug_html(driver, name_prefix: str, logger):
    """Zapisuje page_source do pliku w OUTPUT_DIR i zwraca ścieżkę."""
    filename = f"{name_prefix}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    path = os.path.join(OUTPUT_DIR, filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        logger.info(f"Zapisano debug HTML: {path}")
    except Exception as e:
        logger.error(f"Nie udało się zapisać debug HTML: {e}")
    return path


def human_like_actions(driver):
    """Proste akcje 'ludzkie' — scroll, drobne ruchy myszką."""
    try:
        # scroll w dół i do góry
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.3);")
        time.sleep(random.uniform(0.2, 0.6))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.6);")
        time.sleep(random.uniform(0.2, 0.6))
        driver.execute_script("window.scrollTo(0, 0);")
        # drobne ruchy myszki (jeśli dostępne ActionChains)
        try:
            actions = ActionChains(driver)
            actions.move_by_offset(random.randint(1, 50), random.randint(1, 50)).perform()
        except Exception:
            pass
    except Exception:
        # nie krytyczne
        pass


def get_train_data(date: str, logger) -> list:
    logger.info(f"Rozpoczęto pobieranie danych o pociągach na dzień: {date}")
    data = []
    page_num = 1
    max_retries = 3

    opts = Options()
    # opts.add_argument("--headless=chrome")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=pl-PL")
    # rotuj UA
    opts.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    # minimalne maskowanie selenium
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    try:
        driver = webdriver.Chrome(options=opts)
    except WebDriverException as e:
        logger.critical(f"Nie udało się uruchomić Chrome: {e}")
        raise

    try:
        while True:
            url = (
                "https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html"
                f"?location=&date={date}&category%5Beic_premium%5D=eip"
                "&category%5Beic%5D=eic&category%5Bic%5D=ic&category%5Btlk%5D=tlk"
                f"&page={page_num}"
            )
            logger.info(f"Pobieranie danych ze strony {page_num}: {url}")

            success = False

            for attempt in range(1, max_retries + 1):
                # każda próba rotuje UA (ustawia nowe nagłówki dla chroma — poprzez restart drivera)
                if attempt > 1:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    opts = Options()
                    opts.add_argument("--headless=new")
                    opts.add_argument("--no-sandbox")
                    opts.add_argument("--disable-dev-shm-usage")
                    opts.add_argument("--lang=pl-PL")
                    opts.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
                    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
                    opts.add_experimental_option("useAutomationExtension", False)
                    try:
                        driver = webdriver.Chrome(options=opts)
                        logger.info(f"Restart Chromedriver z nowym User-Agent (próba {attempt}).")
                    except WebDriverException as e:
                        logger.warning(f"Nie udało się zrestartować chromedriver: {e}")
                        time.sleep(2)
                        continue

                try:
                    driver.get(url)
                    human_like_actions(driver)

                    wait = WebDriverWait(driver, 12)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                    success = True
                    logger.info(f"Znaleziono element <table> na stronie {page_num}.")
                    break
                except TimeoutException:
                    logger.warning(f"Timeout czekania na tabelę (próba {attempt}/{max_retries}).")
                    save_debug_html(driver, f"page_{page_num}_timeout_attempt{attempt}", logger)
                    time.sleep(1 + random.uniform(0.5, 2.0))
                except WebDriverException as e:
                    logger.warning(f"Selenium error (próba {attempt}/{max_retries}): {e}")
                    save_debug_html(driver, f"page_{page_num}_webdriver_error_attempt{attempt}", logger)
                    time.sleep(1 + random.uniform(0.5, 2.0))

            if not success:
                logger.error("Wszystkie próby dla tej strony nie powiodły się — zapisuję finalny HTML i przerywam.")
                save_debug_html(driver, f"page_{page_num}_failed_final", logger)
                break

            try:
                table = driver.find_element(By.TAG_NAME, "table")
                rows = table.find_elements(By.TAG_NAME, "tr")[1:]
                page_data = []
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if not cells:
                        continue
                    left = [c.text for c in cells[:5]]
                    right = [c.text for c in cells[6:]] if len(cells) > 6 else []
                    page_data.append(left + right)
            except Exception as e:
                logger.error(f"Błąd parsowania tabeli na stronie {page_num}: {e}")
                save_debug_html(driver, f"page_{page_num}_parse_error", logger)
                break

            if not page_data:
                logger.info(f"Brak rekordów na stronie {page_num} — koniec paginacji.")
                break

            logger.info(f"Pobrano {len(page_data)} wierszy ze strony {page_num}.")
            data.append(page_data)
            page_num += 1

            time.sleep(random.uniform(0.6, 1.8))

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    headers = ["domestic", "number", "category", "name", "from", "to", "occupancy", "delay_info", "date"]
    res = []
    for page in data:
        for train in page:
            train_dict = dict(zip_longest(headers, train, fillvalue="n/a"))
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
    try:
        train_data_wo_delays = get_train_data(today, logger)
    except Exception as exc:
        logger.critical(f"Krytyczny błąd podczas pobierania listy pociągów: {exc}")
        sys.exit(1)

    if not train_data_wo_delays:
        logger.warning("Nie udało się pobrać żadnych danych o pociągach. Zamykanie aplikacji.")
        sys.exit(0)

    logger.info("Rozpoczęto proces pobierania informacji o opóźnieniach...")
    data_with_delays = get_delays(train_data_wo_delays, logger)

    now_str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
    output_filename = f"train_data_{now_str}.json"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    logger.info(f"Zapisywanie wszystkich danych do pliku: {output_path}")

    try:
        with open(output_path, "w", encoding="utf-8-sig") as f:
            json.dump(data_with_delays, f, ensure_ascii=False, indent=4)
        logger.info("Zapisanie danych do pliku JSON zakończone pomyślnie.")
    except IOError as e:
        logger.critical(f"Nie udało się zapisać pliku JSON: {e}")

    logger.info("=" * 50)
    logger.info("PROCES SCRAPOWANIA ZAKOŃCZONY")
    logger.info("=" * 50)
