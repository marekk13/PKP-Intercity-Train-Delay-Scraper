import datetime
import json
import logging
import os
import sys
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright, TimeoutError
from playwright_stealth import stealth_sync

from get_delays import get_delays
from logger_config import setup_logging
from save_to_postgres import save_data

def get_train_data(target_date: datetime.date, logger: logging.Logger) -> list:
    """
    Pobiera dane o frekwencji pociągów ze strony intercity.pl.
    """
    logger.info(f"Rozpoczęto pobieranie podstawowych danych o pociągach na dzień: {target_date}")
    all_trains_data = []
    page_num = 1

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            page = context.new_page()
            stealth_sync(page)
        except Exception as e:
            logger.critical(f"Nie udało się zainicjować przeglądarki Playwright: {e}")
            return []

        while True:
            url = (
                f"https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html?"
                f"location=&date={target_date}&category%5Beic_premium%5D=eip"
                f"&category%5Beic%5D=eic&category%5Bic%5D=ic&category%5Btlk%5D=tlk&page={page_num}"
            )
            logger.info(f"Pobieranie danych ze strony {page_num}: {url}")

            try:
                page.goto(url, timeout=30000)

                table_selector = "table.table"
                logger.info(f"Oczekiwanie na selektor: '{table_selector}'")
                table = page.wait_for_selector(table_selector, timeout=15000)

            except TimeoutError:
                logger.error(f"Na stronie {page_num} nie znaleziono tabeli. Uruchamiam diagnostykę.")

                screenshot_path = f"debug_screenshot_page_{page_num}.png"
                page.screenshot(path=screenshot_path, full_page=True)
                logger.info(f"Zapisano zrzut ekranu do pliku: {screenshot_path}")

                html_path = f"debug_page_content_{page_num}.html"
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(page.content())
                logger.info(f"Zapisano kod HTML strony do pliku: {html_path}")

                logger.warning("Diagnostyka zakończona. Prawdopodobnie to koniec wyników lub błąd selektora.")
                break

            rows = table.query_selector_all("tr")[1:]  # Pomijamy nagłówek

            if not rows:
                logger.info(f"Tabela na stronie {page_num} jest pusta. Zakończono pobieranie.")
                break

            page_data = []
            for row in rows:
                cells = row.query_selector_all("td")
                if not cells:
                    continue
                left = [c.inner_text().strip() for c in cells[:5]]
                right = [c.inner_text().strip() for c in cells[6:]] if len(cells) > 6 else []
                page_data.append(left + right)

            all_trains_data.extend(page_data)
            logger.info(f"Pobrano {len(page_data)} pociągów ze strony {page_num}. Łącznie: {len(all_trains_data)}")
            page_num += 1

        browser.close()

    headers_data = [
        "domestic", "number", "category", "name", "from", "to",
        "occupancy", "delay_info", "date"
    ]

    result_list = []
    for train_row in all_trains_data:
        train_dict = dict(zip(headers_data, train_row))
        train_dict["date"] = target_date.strftime("%Y-%m-%d")
        result_list.append(train_dict)

    logger.info(f"Pobrano łącznie podstawowe dane dla {len(result_list)} pociągów.")
    return result_list


if __name__ == "__main__":
    # 1. Konfiguracja loggera
    logger = setup_logging()

    logger.info("=" * 50)
    logger.info("ROZPOCZĘTO PROCES SCRAPOWANIA")
    logger.info("=" * 50)

    # 2. Pobranie danych podstawowych
    warsaw_timezone = ZoneInfo("Europe/Warsaw")
    now = datetime.datetime.now(warsaw_timezone)
    today = now.date()

    if not train_data_wo_delays:
        logger.warning("Nie udało się pobrać żadnych danych o pociągach. Zamykanie aplikacji.")
        sys.exit(0)

    # 3. Pobranie informacji o opóźnieniach
    logger.info("Rozpoczęto proces pobierania informacji o opóźnieniach...")
    data_with_delays = get_delays(train_data_wo_delays, logger)

    # 4. Zapis wyników do pliku JSON
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    now_str = now.strftime("%Y-%m-%d-%H%M")
    output_filename = os.path.join(output_dir, f"train_data_{now_str}.json")
    logger.info(f"Zapisywanie wszystkich danych do pliku: {output_filename}")

    try:
        with open(output_filename, "w", encoding="utf-8-sig") as f:
            json.dump(data_with_delays, f, ensure_ascii=False, indent=4)
        logger.info("Zapisywanie danych do pliku JSON zakończone pomyślnie.")
    except IOError as e:
        logger.critical(f"Nie udało się zapisać pliku JSON: {e}")

    logger.info("Rozpoczęto proces wysyłania danych do Supabase...")
    save_data(data_with_delays, logger)

    logger.info("=" * 50)
    logger.info("PROCES SCRAPOWANIA ZAKOŃCZONY")
    logger.info("=" * 50)