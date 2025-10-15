import re
import time
import logging
import random

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, NoSuchElementException, \
    StaleElementReferenceException, WebDriverException

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


def get_train_details(driver, wait, train_number, logger):
    """Pobiera szczegółowe dane o trasie pociągu."""
    logger.info(f"Pobieranie danych dla pociągu nr: {train_number}")

    # 1. numer pociągu
    input_box = wait.until(EC.element_to_be_clickable((By.ID, "ftn-number")))
    input_box.clear()
    input_box.send_keys(train_number)

    # 2. klikamy „Szukaj”
    search_button = wait.until(EC.element_to_be_clickable((By.ID, "ftn-search")))
    search_button.click()

    # 3. sprawdzenie, czy jest komunikat "brak kursujących pociągów"
    for attempt in range(3):
        try:
            h3_elems = driver.find_elements(By.CSS_SELECTOR, "h3")
            if h3_elems:
                text = h3_elems[0].text
                if "W obecnej dobie brak kursujących pociągów o szukanym numerze" in text:
                    logger.warning(
                        f"Nie znaleziono pociągu o numerze {train_number}. Komunikat strony: '{text.strip()}'")
                    return "N/A"
            break  # Success, exit retry loop
        except StaleElementReferenceException:
            if attempt < 2:
                time.sleep(0.5)
            else:
                logger.error(
                    "StaleElementReferenceException: nie można pobrać komunikatu o braku pociągów po kilku próbach.")
                return "N/A"

    # 4. Sprawdzenie błędu nieprawidłowego numeru
    try:
        err = driver.find_element(By.CSS_SELECTOR, "div.param-error")
        text = err.text
        if "Wpisany numer pociągu jest nieprawidłowy" in text:
            logger.warning(f"Dla numeru {train_number} znaleziono błąd: 'Wpisany numer pociągu jest nieprawidłowy'")
            return "N/A"
    except (NoSuchElementException, StaleElementReferenceException):
        pass

    # 5. Sprawdzenie, czy wynik to inny numer pociągu
    for attempt in range(3):
        try:
            search_train_elems = driver.find_elements(By.CSS_SELECTOR, "div.col-1.col-6--phone strong span")
            if not search_train_elems:
                logger.warning(f"Nie znaleziono żadnych wyników dla numeru {train_number}.")
                return "N/A"
            search_train_nr = search_train_elems[0].text
            if abs(int(search_train_nr) - int(train_number)) > 1:
                logger.warning(
                    f"Nie znaleziono pociągu o numerze {train_number}. Zamiast tego znaleziono: {search_train_nr}")
                return "N/A"
            break
        except StaleElementReferenceException:
            if attempt < 2:
                time.sleep(0.5)
            else:
                logger.error("StaleElementReferenceException: nie można pobrać numeru pociągu po kilku próbach.")
                return "N/A"

    # 6. Kliknięcie w szczegóły trasy
    for attempt in range(3):
        try:
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.item-details.loadScr")))
            all_details = driver.find_elements(By.CSS_SELECTOR, "a.item-details.loadScr")
            if all_details:
                all_details[0].click()
            else:
                logger.warning(f"Brak wyników do kliknięcia dla pociągu {train_number}.")
                return "not_found"
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.timeline")))
            break
        except (TimeoutException, ElementClickInterceptedException) as e:
            logger.error(
                f"Nie można otworzyć szczegółów trasy dla pociągu {train_number}. Wyjątek: {e.__class__.__name__}")
            return "not_found"
        except StaleElementReferenceException:
            if attempt < 2:
                time.sleep(0.5)
            else:
                logger.error("StaleElementReferenceException: nie można kliknąć szczegółów trasy po kilku próbach.")
                return "not_found"

    # 7. Parsowanie listy stacji
    station_items = driver.find_elements(By.CSS_SELECTOR, "div.timeline__item")
    route_details = []

    for item in station_items:
        # nazwa stacji
        name_elem = item.find_elements(By.CSS_SELECTOR, "h3.timeline__content-station")
        full_name_text = name_elem[0].text if name_elem else ""
        station_name = full_name_text.split(":", 1)[-1].strip()

        # czas przyjazdu
        arr_elem = item.find_elements(By.CSS_SELECTOR, "span.timeline__numbers-time__stop")
        arrival_time = arr_elem[0].text.split('\n')[1].strip() if arr_elem and len(
            arr_elem[0].text.split('\n')) > 1 else None

        # czas odjazdu
        dep_elem = item.find_elements(By.CSS_SELECTOR, "span.timeline__numbers-time__start")
        departure_time = dep_elem[0].text.split('\n')[1].strip() if dep_elem and len(
            dep_elem[0].text.split('\n')) > 1 else None

        # opóźnienia
        delay_arr_elem = item.find_elements(By.CSS_SELECTOR,
                                            "span.timeline__numbers-time__stop span.inlinedelay.sdv[class*='kg']")
        delay_depart_elem = item.find_elements(By.CSS_SELECTOR,
                                               "span.timeline__numbers-time__start span.inlinedelay.sdv[class*='kg']")
        delay_minutes_arrival = parse_delay(delay_arr_elem[0].text) if delay_arr_elem else None
        delay_minutes_departure = parse_delay(delay_depart_elem[0].text) if delay_depart_elem else None

        # dystans i czas do następnej
        km_elem = item.find_elements(By.CSS_SELECTOR, "p.timeline__numbers-km")
        info_text = km_elem[0].text if km_elem else ""
        distance_km, travel_time_to_next = parse_distance_and_time_info(info_text)

        # utrudnienia
        difficulties_btn = item.find_elements(By.CSS_SELECTOR, "button[data-window-type='difficulties']")
        station_diff, difficulties_reason = "", ""
        if difficulties_btn:
            data_obj_1_value = difficulties_btn[0].get_attribute("data-obj-1")
            if data_obj_1_value:
                parts = data_obj_1_value.split('$')
                if len(parts) > 0:
                    first_part_elements = parts[0].split('###')
                    if len(first_part_elements) > 1:
                        station_diff = first_part_elements[1]
                if len(parts) > 2:
                    difficulties_reason = parts[2]

        route_details.append({
            "station_name": station_name,
            "arrival_time": arrival_time,
            "departure_time": departure_time,
            "delay_minutes_arrival": delay_minutes_arrival,
            "delay_minutes_departure": delay_minutes_departure,
            "distance_km_from_start_to_next": distance_km,
            "travel_time_from_start_to_next": travel_time_to_next,
            "difficulties_info": [difficulties_reason, station_diff],
        })

    logger.info(f"Pomyślnie pobrano dane dla {len(route_details)} stacji dla pociągu {train_number}.")
    return route_details


def process_single_train(driver, wait, train: dict, logger):
    """
    Pobiera i parsuje dane dla pojedynczego pociągu.
    """
    train_number = train.get("number")
    if not train_number:
        logger.warning("Pominięto pociąg bez numeru w danych wejściowych.")
        return

    try:
        driver.get(URL)

        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "span.find-train-selector"))).click()

        li_element = wait.until(EC.visibility_of_element_located((By.XPATH, "//li[normalize-space()='po numerze']")))
        driver.execute_script("arguments[0].click();", li_element)

        # Pobranie szczegółów
        details = get_train_details(driver, wait, train_number, logger)
        train["delay_info"] = details

    except TimeoutException as e:
        logger.error(f"Timeout podczas przetwarzania pociągu {train_number}. Pomijanie. Błąd: {e}")
        train["delay_info"] = "scraping_timeout"
    except WebDriverException as e:
        logger.error(f"Błąd WebDrivera podczas przetwarzania pociągu {train_number}. Pomijanie. Błąd: {e.msg}")
        train["delay_info"] = "scraping_error"
    except Exception as e:
        logger.error(f"Nieoczekiwany błąd podczas przetwarzania pociągu {train_number}. Pomijanie. Błąd: {e}")
        train["delay_info"] = "unknown_error"


def get_delays(trains_data: list = None, logger=None) -> list:
    if logger is None:
        logger = logging.getLogger(__name__)

    # Ustawienia Selenium
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    opts.add_argument(f'user-agent={user_agent}')

    opts.add_argument('--disable-blink-features=AutomationControlled')
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--profile-directory=Default")
    opts.add_argument("--incognito")
    opts.add_argument("--disable-plugins-discovery")
    opts.add_argument("--start-maximized")

    try:
        driver = webdriver.Chrome(options=opts)
        logger.info("Pomyślnie połączono z serwerem Selenium w kontenerze Docker.")
    except Exception as e:
        logger.critical(f"Nie udało się połączyć z serwerem Selenium. Błąd: {e}")
        for train in trains_data:
            train["delay_info"] = "selenium_connection_error"
        return trains_data

    wait = WebDriverWait(driver, 15)

    try:
        driver.get(URL)
        try:
            # kliknięcie cookies na początku
            cookie_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(.,'Akceptuj') or contains(.,'Zgoda')]"))
            )
            cookie_button.click()
            logger.info("Zaakceptowano cookies.")
        except TimeoutException:
            logger.warning("Banner cookies nie pojawił się lub nie można było go kliknąć.")

        for train in trains_data:
            process_single_train(driver, wait, train, logger)

    except WebDriverException as e:
        logger.critical(f"Krytyczny błąd WebDrivera, proces zostanie przerwany. Błąd: {e.msg}")
    finally:
        driver.quit()
        logger.info("Zakończono działanie przeglądarki Selenium.")

    return trains_data