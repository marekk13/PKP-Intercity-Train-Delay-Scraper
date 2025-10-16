import json
import logging
import random
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync

def setup_logger():
    log_filename = Path(f"scraper_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()]
    )
    return logging.getLogger(), log_filename


def get_train_data(date: str, logger: logging.Logger):
    base_url = (
        "https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html"
        "?location=&date={date}&category%5Beic_premium%5D=eip"
        "&category%5Beic%5D=eic&category%5Bic%5D=ic&category%5Btlk%5D=tlk&page={page}"
    )

    all_data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context()
        page = context.new_page()
        stealth_sync(page)

        page.set_extra_http_headers({
            "Accept-Language": "pl-PL,pl;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        })

        for page_num in range(1, 50):  # maksymalnie 50 stron, można zwiększyć
            url = base_url.format(date=date, page=page_num)
            logger.info(f"Pobieranie danych ze strony {page_num}: {url}")

            try:
                page.goto(url, timeout=60000)
                page.wait_for_selector("table", timeout=15000)
            except PlaywrightTimeoutError:
                logger.warning(f"Strona {page_num} nie zawiera tabeli lub nie wczytała się w czasie. Kończę.")
                break

            try:
                table = page.query_selector("table")
                if not table:
                    logger.warning(f"Nie znaleziono tabeli na stronie {page_num}.")
                    break

                rows = table.query_selector_all("tr")[1:]  # pomiń nagłówek
                page_data = []
                for row in rows:
                    cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
                    if not cells:
                        continue
                    # zachowaj logikę łączenia lewej/prawej strony tabeli
                    left = cells[:5]
                    right = cells[6:] if len(cells) > 6 else []
                    page_data.append(left + right)

                logger.info(f"Pobrano {len(page_data)} rekordów ze strony {page_num}.")
                all_data.extend(page_data)

                if not page.locator("a.page-link.next, a[aria-label='Następna']").is_visible():
                    logger.info("Nie znaleziono kolejnej strony, koniec scrapowania.")
                    break

                time.sleep(random.uniform(1.5, 3.0))  # naturalny delay między stronami

            except Exception as e:
                logger.error(f"Błąd przy pobieraniu strony {page_num}: {e}")
                break

        browser.close()

    return all_data


if __name__ == "__main__":
    logger, log_file = setup_logger()
    logger.info("=" * 50)
    logger.info("ROZPOCZĘTO NOWY PROCES SCRAPOWANIA DANYCH O POCIĄGACH")
    logger.info("=" * 50)

    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Rozpoczęto pobieranie danych o pociągach na dzień: {today}")

    try:
        data = get_train_data(today, logger)
        logger.info(f"Pobrano dane dla {len(data)} pociągów.")

        if data:
            filename = Path(f"train_data_{today}.json")
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Zapisano dane do pliku: {filename}")
        else:
            logger.warning("Nie udało się pobrać danych o pociągach. Zamykanie aplikacji.")

    except Exception as e:
        logger.exception(f"Nieoczekiwany błąd: {e}")
