import re
import time
import logging
import os
from collections import deque
from playwright.sync_api import sync_playwright, TimeoutError, Page
try:
    from playwright_stealth import stealth_sync
    def apply_stealth(page):
        stealth_sync(page)
except ImportError:
    from playwright_stealth import Stealth
    def apply_stealth(page):
        Stealth().apply_stealth_sync(page)

URL = "https://portalpasazera.pl/Wyszukiwarka/Index"

MONITORED_URL_FRAGMENTS = (
    "WyszukajNumerPociagu",
    "ZnajdzPociagNumer",
    "/challenge/",
    "/Wyszukiwarka/Index",
)
RETRYABLE_STATUSES = (
    "page_load_timeout",
    "parsing_error",
    "not_found",
    "scraping_timeout",
    "unknown_error",
    "autocomplete_error",
)
INVALID_NUMBER_STATUSES = ("invalid_train_number", "autocomplete_error")
MAX_CONSECUTIVE_INVALID_NUMBERS = 15


class NetworkTracker:
    """Rejestruje wybrane zdarzenia sieciowe Playwright na potrzeby diagnostyki błędów."""

    def __init__(self, maxlen: int = 30):
        self.events: deque[dict] = deque(maxlen=maxlen)

    def clear(self) -> None:
        self.events.clear()

    @staticmethod
    def _is_monitored(url: str) -> bool:
        return "portalpasazera.pl" in url and any(frag in url for frag in MONITORED_URL_FRAGMENTS)

    @staticmethod
    def _short_path(url: str) -> str:
        path = re.sub(r"^https?://[^/]+", "", url)
        return path[:120] + ("..." if len(path) > 120 else "")

    def on_response(self, response) -> None:
        url = response.url
        if not self._is_monitored(url):
            return

        headers = response.headers or {}
        body_empty = None
        if "WyszukajNumerPociagu" in url and response.status == 200:
            try:
                body_empty = response.text().strip() == "[]"
            except Exception:
                body_empty = None

        self.events.append({
            "method": response.request.method,
            "path": self._short_path(url),
            "status": response.status,
            "challenge": headers.get("x-require-challenge"),
            "body_empty": body_empty,
            "error": None,
        })

    def on_request_failed(self, request) -> None:
        url = request.url
        if not self._is_monitored(url):
            return

        self.events.append({
            "method": request.method,
            "path": self._short_path(url),
            "status": "FAILED",
            "challenge": None,
            "body_empty": None,
            "error": request.failure,
        })

    def has_autocomplete_failure(self) -> bool:
        """Zwraca True, jeśli endpoint autouzupełniania nie zadziałał (błąd HTTP, błąd sieci lub brak odpowiedzi)."""
        ac_events = [e for e in self.events if "WyszukajNumerPociagu" in e["path"]]
        if not ac_events:
            return True

        # Synchroniczne GetTrainNumberValue wysyła zapytanie bez parametrów filter[...], sprawdzamy też ostatnie wywołanie
        exact_events = [e for e in ac_events if "filter[" not in e["path"] and "filter%5B" not in e["path"]]
        relevant = exact_events[-1:] if exact_events else ac_events[-1:]
        last = relevant[0]
        return last["status"] == "FAILED" or (isinstance(last["status"], int) and last["status"] >= 400)

    def format_summary(self, limit: int = 8) -> str:
        if not self.events:
            return "brak zarejestrowanych zapytań"

        parts = []
        for ev in list(self.events)[-limit:]:
            desc = f"{ev['method']} {ev['path']} -> {ev['status']}"
            flags = []
            if ev.get("challenge"):
                flags.append(f"challenge={ev['challenge']}")
            if ev.get("body_empty") is True:
                flags.append("empty=[]")
            if ev.get("error"):
                flags.append(f"err={ev['error']}")
            if flags:
                desc += f" ({', '.join(flags)})"
            parts.append(desc)
        return "; ".join(parts)


def parse_delay(delay_text: str) -> int:
    """Wyciąga liczbę minut opóźnienia z tekstu"""
    if not delay_text:
        return 0
    match = re.search(r'\(\+(\d+)\s*min\)', delay_text, re.IGNORECASE)
    return int(match.group(1)) if match else 0


def parse_distance_and_time_info(info_text: str) -> tuple:
    """Wyciąga dystans i czas przejazdu z tekstu."""
    if not info_text:
        return None, None

    distance_km = None
    travel_time = None

    # Wyszukiwanie dystansu (np. 12.6 km lub 12,6 km) w całym tekście
    distance_match = re.search(r'([\d,\.]+)\s*km', info_text)
    if distance_match:
        distance_km = float(distance_match.group(1).replace(',', '.'))

    # Wyszukiwanie czasu przejazdu (np. 12h:14min) w całym tekście
    time_match = re.search(r'(\d+h:\d+min)', info_text)
    if time_match:
        travel_time = time_match.group(1)

    return distance_km, travel_time


def parse_difficulties(difficulties_text: str) -> tuple:
    """Wyciąga informacje o utrudnieniach i stacji z tekstu."""
    if not difficulties_text:
        return None, None
    parts = difficulties_text.split('\n')
    return parts[1].strip(), parts[2].strip() if len(parts) > 1 else None


def extract_train_numbers(number_str: str) -> list:
    """Wyciąga numery pociągów z tekstu (np. 'IC 1116', '1116/7', 'IC 1116 / 1117', 'TLK 53100')."""
    if not number_str:
        return []

    # Obsługa zapisu relacji z ukośnikiem, np. 1116/7 lub 53100/1
    slash_match = re.search(r'(\d+)\s*/\s*(\d+)', str(number_str))
    if slash_match:
        n1, n2 = slash_match.group(1), slash_match.group(2)
        if len(n2) < len(n1):
            n2_full = n1[:-len(n2)] + n2
            return [int(n1), int(n2_full)]
        return [int(n1), int(n2)]

    return [int(n) for n in re.findall(r'\d+', str(number_str))]


def get_train_details(page: Page, train_number: str, logger: logging.Logger, target_date: str = None, network_tracker: NetworkTracker = None):
    """Pobiera szczegółowe dane o trasie pociągu."""
    if target_date:
        logger.info(f"Pobieranie danych dla pociągu nr: {train_number} z datą: {target_date}")
    else:
        logger.info(f"Pobieranie danych dla pociągu nr: {train_number}")

    train_number_str = str(train_number).strip()
    search_digits = re.search(r'\d+', train_number_str)
    search_query = search_digits.group(0) if search_digits else train_number_str

    input_field = page.locator("#ftnu-number")
    input_field.click()
    input_field.clear()
    input_field.press_sequentially(search_query, delay=100)

    # Wypełnianie kalendarza jeśli podano target_date
    if target_date:
        date_input = page.locator("#ftnu-number-date")
        date_input.fill(target_date)
        date_input.evaluate("el => el.blur()")

    # Usunięcie focusu z pola (blur), co zamyka listę podpowiedzi (autocomplete) bez zamykania modala
    input_field.evaluate("el => el.blur()")

    try:
        page.wait_for_load_state('networkidle', timeout=5000)
    except TimeoutError:
        logger.warning(f"Strona nie osiągnęła stanu 'networkidle' dla pociągu {train_number}. Mimo to kontynuuję.")

    page.locator("#ftnu-search").click()

    try:
        page.wait_for_selector(
            "div.catalog-table, h3:has-text('dobie brak kursujących pociągów'), h3:has-text('brak kursujących pociągów'), div.param-error:has-text('Wpisany numer pociągu jest nieprawidłowy')",
            timeout=15000
        )
    except TimeoutError:
        net_info = f" Sieć: {network_tracker.format_summary()}" if network_tracker else ""
        logger.warning(f"Strona nie załadowała wyników ani komunikatu o błędzie dla pociągu {train_number}.{net_info}")
        return "page_load_timeout"

    no_train_msg = page.locator("h3:has-text('dobie brak kursujących pociągów'), h3:has-text('brak kursujących pociągów')").first
    if no_train_msg.is_visible():
        logger.warning(
            f"Nie znaleziono pociągu o numerze {train_number}. Komunikat strony: '{no_train_msg.inner_text().strip()}'")
        return "N/A"

    invalid_nr_msg = page.locator("div.param-error:has-text('Wpisany numer pociągu jest nieprawidłowy')")
    if invalid_nr_msg.is_visible():
        net_info = f" Sieć: {network_tracker.format_summary()}" if network_tracker else ""
        logger.warning(f"Dla numeru {train_number} znaleziono błąd: 'Wpisany numer pociągu jest nieprawidłowy'.{net_info}")
        if network_tracker is not None and network_tracker.has_autocomplete_failure():
            return "autocomplete_error"
        return "invalid_train_number"

    try:
        row_count = page.locator("div.catalog-table__row").count()
        logger.debug(f"Znaleziono {row_count} wierszy do sprawdzenia.")

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
                    row_numbers = []
                    for s in found_numbers_str:
                        row_numbers.extend(extract_train_numbers(s))

                    target_numbers = extract_train_numbers(train_number)

                    if not row_numbers or not target_numbers:
                        logger.warning(f"W wierszu znaleziono nieprawidłowy format numeru pociągu: {found_numbers_str}")
                        continue

                    is_number_match = any(
                        abs(r_num - t_num) <= 1
                        for r_num in row_numbers
                        for t_num in target_numbers
                    )
                    if is_number_match:
                        logger.debug(f"Znaleziono pasujący pociąg IC w wierszu nr {i + 1}.")
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
    station_items = page.locator("div.timeline--connection div.timeline__item:not(.timeline__item-detour)").all()
    route_details = []

    for item in station_items:
        raw_station_name = item.locator("h3.timeline__content-station").first.inner_text().split(":", 1)[-1].strip()
        station_name = re.sub(r'(?i)\s*przesiadka$', '', raw_station_name).strip()

        # Detekcja stacji odwołanej
        is_cancelled = False
        hidden_span = item.locator("span.visuallyhidden").first
        if hidden_span.count() > 0:
            is_cancelled = "odwołan" in hidden_span.inner_text().lower()

        arrival_time = None
        delay_minutes_arrival = 0
        arrival_locator = item.locator("span.timeline__numbers-time__stop").first
        if arrival_locator.count() > 0:
            arr_time_text = arrival_locator.text_content() or ""
            time_match = re.search(r'\b(\d{2}:\d{2})\b', arr_time_text)
            if time_match:
                arrival_time = time_match.group(1)
            delay_minutes_arrival = parse_delay(arr_time_text)

        departure_time = None
        delay_minutes_departure = 0
        departure_locator = item.locator("span.timeline__numbers-time__start").first
        if departure_locator.count() > 0:
            dep_time_text = departure_locator.text_content() or ""
            time_match = re.search(r'\b(\d{2}:\d{2})\b', dep_time_text)
            if time_match:
                departure_time = time_match.group(1)
            delay_minutes_departure = parse_delay(dep_time_text)

        info_text = ""
        info_locator = item.locator("p.timeline__numbers-km").first
        if info_locator.count() > 0:
            info_text = info_locator.inner_text()
        distance_km, travel_time_to_next = parse_distance_and_time_info(info_text)

        difficulties_btn = item.locator("button[data-window-type='difficulties']").first
        station_diff, difficulties_reason = "", ""
        if difficulties_btn.count() > 0:
            data_obj_1_value = difficulties_btn.get_attribute("data-obj-1")
            if data_obj_1_value:
                parts = data_obj_1_value.split('$')
                if len(parts) > 0:
                    first_part_elements = parts[0].split('###')
                    if len(first_part_elements) > 1:
                        station_diff = first_part_elements[1].lstrip('#').strip()
                if len(parts) > 2:
                    difficulties_reason = parts[2]
                    if '#' in difficulties_reason:
                        reason_parts = difficulties_reason.split('#', 1)
                        difficulties_reason = reason_parts[0].strip()
                        if len(reason_parts) > 1 and reason_parts[1].strip():
                            station_diff = reason_parts[1].strip()

        route_details.append({
            "station_name": station_name, "arrival_time": arrival_time, "departure_time": departure_time,
            "delay_minutes_arrival": delay_minutes_arrival, "delay_minutes_departure": delay_minutes_departure,
            "distance_km_from_start_to_next": distance_km, "travel_time_from_start_to_next": travel_time_to_next,
            "difficulties_info": [difficulties_reason, station_diff],
            "is_cancelled": is_cancelled
        })

    # Usunięcie duplikatów stacji (np. przy częściowo odwołanych pociągach)
    merged_route_details = []
    station_index_map = {}

    for stop in route_details:
        name = stop["station_name"]
        if name not in station_index_map:
            station_index_map[name] = len(merged_route_details)
            merged_route_details.append(stop)
        else:
            existing_stop = merged_route_details[station_index_map[name]]

            # Scalanie czasów i opóźnień
            if stop.get("arrival_time") and not existing_stop.get("arrival_time"):
                existing_stop["arrival_time"] = stop["arrival_time"]
                existing_stop["delay_minutes_arrival"] = stop.get("delay_minutes_arrival", 0)

            if stop.get("departure_time") and not existing_stop.get("departure_time"):
                existing_stop["departure_time"] = stop["departure_time"]
                existing_stop["delay_minutes_departure"] = stop.get("delay_minutes_departure", 0)

            # Stacja nie jest odwołana, jeśli choć jedno jej wystąpienie nie było odwołane
            existing_stop["is_cancelled"] = existing_stop["is_cancelled"] and stop["is_cancelled"]

            # Scalanie informacji o utrudnieniach
            for idx in range(len(existing_stop["difficulties_info"])):
                if idx < len(stop["difficulties_info"]):
                    if stop["difficulties_info"][idx] and not existing_stop["difficulties_info"][idx]:
                        existing_stop["difficulties_info"][idx] = stop["difficulties_info"][idx]

            # Scalanie odległości i czasu przejazdu do następnej stacji
            if stop.get("distance_km_from_start_to_next") is not None and existing_stop.get("distance_km_from_start_to_next") is None:
                existing_stop["distance_km_from_start_to_next"] = stop["distance_km_from_start_to_next"]
            if stop.get("travel_time_from_start_to_next") is not None and existing_stop.get("travel_time_from_start_to_next") is None:
                existing_stop["travel_time_from_start_to_next"] = stop["travel_time_from_start_to_next"]

    logger.info(f"Pomyślnie pobrano dane dla {len(merged_route_details)} stacji dla pociągu {train_number} (po scaleniu {len(route_details)} -> {len(merged_route_details)}).")

    return merged_route_details


def process_single_train(page: Page, train: dict, logger: logging.Logger, target_date: str = None, network_tracker: NetworkTracker = None):
    """Pobiera i parsuje dane dla pojedynczego pociągu."""
    train_number = train.get("number")
    if not train_number:
        logger.warning("Pominięto pociąg bez numeru w danych wejściowych.")
        return

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        if network_tracker is not None:
            network_tracker.clear()
        try:
            page.goto(URL, timeout=30000, wait_until='domcontentloaded')
    
            # Wybór wyszukiwania po numerze
            page.locator("span.find-train-selector").click()
            page.locator("li:has-text('po numerze')").click()
    
            if network_tracker is not None:
                details = get_train_details(page, train_number, logger, target_date, network_tracker=network_tracker)
            else:
                details = get_train_details(page, train_number, logger, target_date)
            train["delay_info"] = details
            if isinstance(details, list) and details:
                is_run_cancelled = all(stop.get("is_cancelled", False) for stop in details)
                train["is_cancelled"] = is_run_cancelled
            else:
                train["is_cancelled"] = False
            
            # Przerywamy pętlę retries jeśli udało się pobrać dane lub dostaliśmy stan w którym retry nie pomoże
            if not (isinstance(details, str) and details in RETRYABLE_STATUSES):
                break
                
            if attempt < max_retries:
                logger.info(f"Otrzymano błąd lub brak danych ({details}). Próba ponowienia ({attempt}/{max_retries})...")
                time.sleep(2)
            else:
                logger.warning(f"Nie udało się pobrać danych dla pociągu {train_number} po {max_retries} próbach.")

        except TimeoutError as e:
            net_info = f" Sieć: {network_tracker.format_summary()}" if network_tracker else ""
            if attempt < max_retries:
                logger.error(f"Timeout podczas przetwarzania pociągu {train_number} (próba {attempt}/{max_retries}). Ponawiam. Błąd: {e}.{net_info}")
                time.sleep(2)
            else:
                logger.error(f"Timeout podczas przetwarzania pociągu {train_number}. Osiągnięto limit prób. Błąd: {e}.{net_info}")
                train["delay_info"] = "scraping_timeout"
        except Exception as e:
            net_info = f" Sieć: {network_tracker.format_summary()}" if network_tracker else ""
            if attempt < max_retries:
                logger.error(f"Nieoczekiwany błąd podczas przetwarzania pociągu {train_number} (próba {attempt}/{max_retries}). Ponawiam. Błąd: {e}.{net_info}")
                time.sleep(2)
            else:
                logger.error(f"Nieoczekiwany błąd podczas przetwarzania pociągu {train_number}. Osiągnięto limit prób. Błąd: {e}.{net_info}", exc_info=True)
                train["delay_info"] = "unknown_error"


def get_delays(trains_data: list = None, logger=None, max_consecutive_invalid_numbers: int = MAX_CONSECUTIVE_INVALID_NUMBERS) -> list:
    if logger is None:
        logger = logging.getLogger(__name__)

    with sync_playwright() as p:
        try:
            try:
                from playwright_stealth import Stealth
                Stealth().hook_playwright_context(p)
            except ImportError:
                pass

            proxy_host = os.environ.get("PROXY_HOST")
            proxy_port = os.environ.get("PROXY_PORT")
            proxy_user = os.environ.get("PROXY_USER")
            proxy_pass = os.environ.get("PROXY_PASSWORD")

            
            launch_args = {"headless": True}
            context_args = {
                "locale": "pl-PL",
                "timezone_id": "Europe/Warsaw",
                "ignore_https_errors": True
            }

            if proxy_host and proxy_port:
                proxy_config = {
                    "server": f"http://{proxy_host}:{proxy_port}",
                }
                if proxy_user and proxy_pass:
                    proxy_config["username"] = proxy_user
                    proxy_config["password"] = proxy_pass
                launch_args["proxy"] = proxy_config
                logger.info(f"Uruchamianie Playwright z proxy: {proxy_host}:{proxy_port}")

            browser = p.chromium.launch(**launch_args)
            context = browser.new_context(**context_args)
            page = context.new_page()

            def block_unnecessary_resources(route):
                if route.request.resource_type in ["image", "font", "media"]:
                    route.abort()
                elif any(domain in route.request.url.lower() for domain in ["google-analytics", "googletagmanager", "hotjar", "facebook", "doubleclick", "analytics", "pixel"]):
                    route.abort()
                else:
                    route.continue_()

            page.route("**/*", block_unnecessary_resources)
            network_tracker = NetworkTracker()
            page.on("response", network_tracker.on_response)
            page.on("requestfailed", network_tracker.on_request_failed)
            apply_stealth(page)
            logger.info("Pomyślnie uruchomiono przeglądarkę Playwright z filtrowaniem zasobów.")
        except Exception as e:
            logger.critical(f"Nie udało się uruchomić przeglądarki Playwright. Błąd: {e}")
            for train in trains_data:
                train["delay_info"] = "playwright_connection_error"
            return trains_data

        try:
            network_tracker.clear()
            page.goto(URL, timeout=30000)
            try:
                # Kliknięcie cookies na początku
                cookie_button = page.locator("button:has-text('Akceptuj wszystkie'), button:has-text('Akceptuj'), button:has-text('Zgoda')").first
                cookie_button.click(timeout=5000)
                logger.info("Zaakceptowano cookies.")
            except TimeoutError:
                logger.warning(
                    f"Banner cookies nie pojawił się lub nie można było go kliknąć. Sieć: {network_tracker.format_summary()}"
                )

            consecutive_invalid_numbers = 0
            for train in trains_data:
                target_date = train.get("target_date")
                process_single_train(page, train, logger, target_date=target_date, network_tracker=network_tracker)

                status = train.get("delay_info")
                if isinstance(status, str) and status in INVALID_NUMBER_STATUSES:
                    consecutive_invalid_numbers += 1
                    if consecutive_invalid_numbers >= max_consecutive_invalid_numbers:
                        msg = (
                            f"Przerwano scrapowanie: wykryto {consecutive_invalid_numbers} kolejnych błędów "
                            f"'Wpisany numer pociągu jest nieprawidłowy' / błędów wyszukiwarki. "
                            f"Ostatnia sieć: {network_tracker.format_summary()}"
                        )
                        logger.critical(msg)
                        raise RuntimeError(msg)
                elif isinstance(status, list) or status == "N/A":
                    consecutive_invalid_numbers = 0
        finally:
            browser.close()
            logger.info("Zakończono działanie przeglądarki Playwright.")

    return trains_data