import re
import time
import logging
from playwright.sync_api import sync_playwright, TimeoutError, Page

URL = "https://portalpasazera.pl/Wyszukiwarka/Index"


def parse_delay(delay_text: str) -> int:
    """Wyciąga liczbę minut opóźnienia z tekstu"""
    if not delay_text:
        return 0
    match = re.search(r'\(\+(\d+)\s*min\)', delay_text)
    return int(match.group(1)) if match else 0


def parse_distance_and_time_info(info_text: str) -> tuple:
    """Wyciąga dystans i czas przejazdu z tekstu rozdzielonego nową linią."""
    if not info_text:
        return None, None

    parts = info_text.strip().split('\n')
    distance_km = None
    travel_time = None

    if len(parts) > 1:
        distance_match = re.search(r'([\d,\.]+)\s*km', parts[1])
        if distance_match:
            distance_km = float(distance_match.group(1).replace(',', '.'))

    if len(parts) > 3:
        time_match = re.search(r'(\d+h:\d+min)', parts[3])
        if time_match:
            travel_time = time_match.group(1)

    return distance_km, travel_time


def parse_difficulties(difficulties_text: str) -> tuple:
    """Wyciąga informacje o utrudnieniach i stacji z tekstu."""
    if not difficulties_text:
        return None, None
    parts = difficulties_text.split('\n')
    return parts[1].strip(), parts[2].strip() if len(parts) > 1 else None


def get_train_details(page: Page, train_number: str, logger: logging.Logger):
    """Pobiera szczegółowe dane o trasie pociągu."""
    logger.info(f"Pobieranie danych dla pociągu nr: {train_number}")

    page.locator("#ftn-number").fill(train_number)

    page.locator("#ftn-search").click()

    try:
        page.wait_for_selector(
            "div.catalog-table, h3:has-text('W obecnej dobie brak kursujących pociągów'), div.param-error:has-text('Wpisany numer pociągu jest nieprawidłowy')",
            timeout=15000
        )
    except TimeoutError:
        logger.warning(f"Strona nie załadowała wyników ani komunikatu o błędzie dla pociągu {train_number}.")
        return "page_load_timeout"

    no_train_msg = page.locator("h3:has-text('W obecnej dobie brak kursujących pociągów')")
    if no_train_msg.is_visible():
        logger.warning(
            f"Nie znaleziono pociągu o numerze {train_number}. Komunikat strony: '{no_train_msg.inner_text().strip()}'")
        return "N/A"

    invalid_nr_msg = page.locator("div.param-error:has-text('Wpisany numer pociągu jest nieprawidłowy')")
    if invalid_nr_msg.is_visible():
        logger.warning(f"Dla numeru {train_number} znaleziono błąd: 'Wpisany numer pociągu jest nieprawidłowy'")
        return "N/A"

    try:
        row_count = page.locator("div.catalog-table__row").count()
        logger.info(f"Znaleziono {row_count} wierszy do sprawdzenia.")

        target_row = None
        for i in range(row_count):
            row = page.locator("div.catalog-table__row").nth(i)

            carrier_element = row.locator("div:has(> span.item-label:has-text('Przewoźnik')) > strong.item-value")
            numbers_container = row.locator("div:has(> span.item-label:has-text('Nr pociągu')) > strong.item-value")

            if carrier_element.count() == 0 or numbers_container.count() == 0:
                logger.warning(f"Wiersz {i + 1} ma niekompletną strukturę, pomijam.")
                continue

            carrier = carrier_element.inner_text().strip()

            if carrier == "IC":
                found_numbers_str = numbers_container.locator("span").all_inner_texts()

                if not found_numbers_str:
                    found_numbers_str = [numbers_container.inner_text().strip()]

                try:
                    is_number_match = any(abs(int(num) - int(train_number)) <= 1 for num in found_numbers_str)
                    if is_number_match:
                        logger.info(f"Znaleziono pasujący pociąg IC w wierszu nr {i + 1}.")
                        target_row = row
                        break
                except (ValueError, TypeError):
                    logger.warning(f"W wierszu znaleziono nieprawidłowy format numeru pociągu: {found_numbers_str}")
                    continue

    except Exception as e:
        logger.error(f"Wystąpił nieoczekiwany błąd podczas analizowania wierszy tabeli: {e}", exc_info=True)
        return "parsing_error"

    if not target_row:
        logger.warning(
            f"Przeanalizowano wszystkie wiersze, ale nie znaleziono pasującego pociągu IC dla numeru {train_number}.")
        return "N/A"

    details_link = target_row.locator("a.item-details.loadScr")
    try:
        details_link.click()
        page.wait_for_selector("div.timeline", timeout=15000)
    except TimeoutError as e:
        logger.error(f"Nie można otworzyć szczegółów trasy dla pociągu {train_number}. Wyjątek: {e.__class__.__name__}")
        return "not_found"

    # parsowanie listy stacji
    station_items = page.locator("div.timeline__item").all()
    route_details = []

    for item in station_items:
        station_name = item.locator("h3.timeline__content-station").inner_text().split(":", 1)[-1].strip()

        arrival_time = None
        arrival_locator = item.locator("span.timeline__numbers-time__stop")
        if arrival_locator.count() > 0:
            arr_time_text = arrival_locator.inner_text()
            arrival_time = arr_time_text.split('\n')[1].strip() if '\n' in arr_time_text else None

        departure_time = None
        departure_locator = item.locator("span.timeline__numbers-time__start")
        if departure_locator.count() > 0:
            dep_time_text = departure_locator.inner_text()
            departure_time = dep_time_text.split('\n')[1].strip() if '\n' in dep_time_text else None

        delay_minutes_arrival = None
        delay_arr_locator = item.locator("span.timeline__numbers-time__stop span.inlinedelay")
        if delay_arr_locator.count() > 0:
            all_texts = delay_arr_locator.all_inner_texts()
            delay_minutes_arrival = parse_delay(all_texts[-1]) if all_texts else 0

        delay_minutes_departure = None
        delay_depart_locator = item.locator("span.timeline__numbers-time__start span.inlinedelay")
        if delay_depart_locator.count() > 0:
            all_texts = delay_depart_locator.all_inner_texts()
            delay_minutes_departure = parse_delay(all_texts[-1]) if all_texts else 0

        info_text = ""
        info_locator = item.locator("p.timeline__numbers-km")
        if info_locator.count() > 0:
            info_text = info_locator.inner_text()
        distance_km, travel_time_to_next = parse_distance_and_time_info(info_text)

        difficulties_btn = item.locator("button[data-window-type='difficulties']")
        station_diff, difficulties_reason = "", ""
        if difficulties_btn.count() > 0:
            data_obj_1_value = difficulties_btn.get_attribute("data-obj-1")
            if data_obj_1_value:
                parts = data_obj_1_value.split('$')
                if len(parts) > 0:
                    first_part_elements = parts[0].split('###')
                    if len(first_part_elements) > 1: station_diff = first_part_elements[1]
                if len(parts) > 2: difficulties_reason = parts[2]

        route_details.append({
            "station_name": station_name, "arrival_time": arrival_time, "departure_time": departure_time,
            "delay_minutes_arrival": delay_minutes_arrival, "delay_minutes_departure": delay_minutes_departure,
            "distance_km_from_start_to_next": distance_km, "travel_time_from_start_to_next": travel_time_to_next,
            "difficulties_info": [difficulties_reason, station_diff],
        })

    logger.info(f"Pomyślnie pobrano dane dla {len(route_details)} stacji dla pociągu {train_number}.")

    return route_details


def process_single_train(page: Page, train: dict, logger: logging.Logger):
    """Pobiera i parsuje dane dla pojedynczego pociągu."""
    train_number = train.get("number")
    if not train_number:
        logger.warning("Pominięto pociąg bez numeru w danych wejściowych.")
        return

    try:
        page.goto(URL, timeout=30000, wait_until='domcontentloaded')

        # Wybór wyszukiwania po numerze
        page.locator("span.find-train-selector").click()
        page.locator("li:has-text('po numerze')").click()

        details = get_train_details(page, train_number, logger)
        train["delay_info"] = details

    except TimeoutError as e:
        logger.error(f"Timeout podczas przetwarzania pociągu {train_number}. Pomijanie. Błąd: {e}")
        train["delay_info"] = "scraping_timeout"
    except Exception as e:
        logger.error(f"Nieoczekiwany błąd podczas przetwarzania pociągu {train_number}. Pomijanie. Błąd: {e}",
                     exc_info=True)
        train["delay_info"] = "unknown_error"


def get_delays(trains_data: list = None, logger=None) -> list:
    if logger is None:
        logger = logging.getLogger(__name__)

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            page = context.new_page()
            logger.info("Pomyślnie uruchomiono przeglądarkę Playwright.")
        except Exception as e:
            logger.critical(f"Nie udało się uruchomić przeglądarki Playwright. Błąd: {e}")
            for train in trains_data:
                train["delay_info"] = "playwright_connection_error"
            return trains_data

        page.goto(URL, timeout=30000)
        try:
            # Kliknięcie cookies na początku
            cookie_button = page.locator("button:has-text('Akceptuj'), button:has-text('Zgoda')").first
            cookie_button.click(timeout=5000)
            logger.info("Zaakceptowano cookies.")
        except TimeoutError:
            logger.warning("Banner cookies nie pojawił się lub nie można było go kliknąć.")

        for train in trains_data:
            process_single_train(page, train, logger)

        browser.close()
        logger.info("Zakończono działanie przeglądarki Playwright.")

    return trains_data