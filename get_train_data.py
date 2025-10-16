import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth

# Konfiguracja logów
log_filename = f"scraper_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger().addHandler(logging.StreamHandler())

# Stałe
BASE_URL = "https://www.intercity.pl/pl/site/dla-pasazera/informacje/frekwencja.html"
DATE = datetime.now().strftime("%Y-%m-%d")

# ============================================================

async def fetch_page_data(page, page_number):
    url = (
        f"{BASE_URL}?location=&date={DATE}"
        f"&category%5Beic_premium%5D=eip"
        f"&category%5Beic%5D=eic"
        f"&category%5Bic%5D=ic"
        f"&category%5Btlk%5D=tlk&page={page_number}"
    )
    logging.info(f"Pobieranie danych ze strony {page_number}: {url}")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_selector("table", timeout=5000)
        html = await page.content()

        # Parsowanie tabeli po stronie przeglądarki
        rows = await page.query_selector_all("table tr")
        page_data = []
        for row in rows[1:]:
            cells = await row.query_selector_all("td")
            if not cells:
                continue
            values = [await c.inner_text() for c in cells]
            left = values[:5]
            right = values[6:] if len(values) > 6 else []
            page_data.append(left + right)

        return page_data

    except PlaywrightTimeoutError:
        logging.error(f"Timeout na stronie {page_number}")
        html = await page.content()
        Path(f"error_page_{page_number}.html").write_text(html)
        return []

    except Exception as e:
        logging.error(f"Błąd przy pobieraniu strony {page_number}: {e}")
        html = await page.content()
        Path(f"error_page_{page_number}.html").write_text(html)
        return []

# ============================================================

async def main():
    logging.info("=" * 60)
    logging.info("ROZPOCZĘTO NOWY PROCES SCRAPOWANIA DANYCH O POCIĄGACH")
    logging.info("=" * 60)
    logging.info(f"Rozpoczęto pobieranie danych o pociągach na dzień: {DATE}")

    data = []
    stealth = Stealth()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
        )

        page = await context.new_page()
        await stealth.apply_stealth_async(page)

        for page_number in range(1, 20):
            page_data = await fetch_page_data(page, page_number)
            if not page_data:
                break
            data.extend(page_data)

        await browser.close()

    # Zapis wyników
    output_filename = f"train_data_{DATE}.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"Pobrano dane dla {len(data)} pociągów.")
    if not data:
        logging.warning("Nie udało się pobrać danych o pociągach. Zamykanie aplikacji.")
    else:
        logging.info(f"Dane zapisano do: {output_filename}")


if __name__ == "__main__":
    asyncio.run(main())
