import logging
from datetime import date
from playwright.sync_api import sync_playwright, TimeoutError
from playwright_stealth import stealth_sync

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_train_data(target_date: date):
    url = (
        f"https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html?"
        f"location=&date={target_date}&category%5Beic_premium%5D=eip"
        f"&category%5Beic%5D=eic&category%5Bic%5D=ic&category%5Btlk%5D=tlk&page=1"
    )

    data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # stealth
        stealth_sync(page)

        logger.info(f"Pobieranie danych ze strony 1: {url}")
        response = page.goto(url, timeout=30000)
        if not response:
            logger.warning("Brak odpowiedzi HTTP ze strony")
        else:
            logger.info(f"Status HTTP: {response.status}")
            logger.info(f"Response headers: {response.headers}")

        # Dajemy czas na JS + renderowanie tabeli
        try:
            table = page.wait_for_selector("table", timeout=15000)
        except TimeoutError:
            logger.warning("Strona nie zawiera tabeli lub nie wczytała się w czasie 15s. Kończę.")
            logger.info("=== Zawartość strony (pierwsze 3000 znaków) ===")
            page_source = page.content()
            logger.info(page_source[:3000])
            browser.close()
            return []

        rows = table.query_selector_all("tr")[1:]  # pomijamy header
        for row in rows:
            cells = row.query_selector_all("td")
            if not cells:
                continue
            left = [c.inner_text().strip() for c in cells[:5]]
            right = [c.inner_text().strip() for c in cells[6:]] if len(cells) > 6 else []
            data.append(left + right)

        logger.info(f"Pobrano dane dla {len(data)} pociągów.")
        browser.close()

    return data

if __name__ == "__main__":
    today = date.today()
    try:
        train_data = get_train_data(today)
        if not train_data:
            logger.warning("Nie udało się pobrać danych o pociągach. Zamykanie aplikacji.")
        else:
            logger.info(f"Dane: {train_data[:3]} ...")  # pokazujemy przykładowe pierwsze 3 wiersze
    except Exception as e:
        logger.critical(f"Wystąpił błąd: {e}", exc_info=True)
