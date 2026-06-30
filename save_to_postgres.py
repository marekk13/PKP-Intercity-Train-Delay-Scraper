import os
import json
import logging
import urllib.request
import re
from supabase import create_client, Client
from typing import Dict, List, Any, Tuple, Set

def load_station_aliases() -> Dict[str, str]:
    aliases_path = os.path.join(os.path.dirname(__file__), 'docs', 'misc', 'station_aliases.json')
    if os.path.exists(aliases_path):
        try:
            with open(aliases_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Błąd ładowania pliku z aliasami stacji: {e}")
    return {}

STATION_NAME_OVERRIDES = load_station_aliases()

def _normalize_station_key(s: str) -> str:
    """Zamienia myślniki i białe znaki na pojedynczą spację, małe litery, strip."""
    return re.sub(r'[-\s]+', ' ', s).lower().strip()

STATION_NAME_OVERRIDES_NORMALIZED = {
    _normalize_station_key(k): v for k, v in STATION_NAME_OVERRIDES.items()
}

TRAIN_NAME_OVERRIDES = {
    "BACZYNSKI": "Baczyński",
    "BALTYK": "Bałtyk",
    "BLATNIA": "Błatnia",
    "BOLESLAW PRUS": "Bolesław Prus",
    "CHELMIANIN": "Chełmianin",
    "CHELMONSKI": "Chełmoński",
    "DABROWSKA": "Dąbrowska",
    "DASZYNSKI": "Daszyński",
    "DEBOWIEC": "Dębowiec",
    "DRWECA": "Drwęca",
    "FALAT": "Fałat",
    "GALCZYNSKI": "Gałczyński",
    "GORNIK": "Górnik",
    "GORSKI": "Górski",
    "HANCZA": "Hańcza",
    "JACWING": "Jaćwing",
    "JAGIELLO": "Jagiełło",
    "KARLOWICZ": "Karłowicz",
    "KILINSKI": "Kiliński",
    "KOZIOLEK": "Koziołek",
    "KRASINSKI": "Krasiński",
    "LEMPICKA": "Łempicka",
    "LESMIAN": "Leśmian",
    "LOKIETEK": "Łokietek",
    "LUKASIEWICZ": "Łukasiewicz",
    "LYSICA": "Łysica",
    "LUZYCE": "Łużyce",
    "MALOPOLSKA": "Małopolska",
    "MARSZALEK PILSUDSKI": "Marszałek Piłsudski",
    "NALKOWSKA": "Nałkowska",
    "NOTEC": "Noteć",
    "OLENKA": "Oleńka",
    "ORLOWICZ": "Orłowicz",
    "PARSETA": "Parsęta",
    "POBRZEZE": "Pobrzeże",
    "POGORZE": "Pogórze",
    "POLONINY": "Połoniny",
    "PORAZINSKA": "Porazińska",
    "POWISLE": "Powiśle",
    "PRZASNICZKA": "Przaśniczka",
    "PRZEMYSLANIN": "Przemyślanin",
    "PULASKI": "Pułaski",
    "RADZIWILL": "Radziwiłł",
    "SLAZAK": "Ślązak",
    "SLEZA": "Ślęża",
    "SLOWACKI": "Słowacki",
    "SLOWINIEC": "Słowiniec",
    "SLUPIA": "Słupia",
    "SNIEZKA": "Śnieżka",
    "STANCZYK": "Stańczyk",
    "STARZYNSKI": "Starzyński",
    "STRYJENSKA": "Stryjeńska",
    "SWAROZYC": "Swarożyc",
    "WISLOK": "Wisłok",
    "WLOKNIARZ": "Włókniarz",
    "WYBRZEZE": "Wybrzeże",
    "WYCZOLKOWSKI": "Wyczółkowski",
    "WYSPIANSKI": "Wyspiański",
    "ZEGLARZ": "Żeglarz",
    "ZEROMSKI": "Żeromski",
    "ZIELONOGORZANIN": "Zielonogórzanin",
    "ZUBR": "Żubr",
    "ZULAWY": "Żuławy",
    "ZYLICA": "Żylica",
}

def _get_or_create_service_id(supabase: Client, service_data: Dict[str, Any], cache: Dict[Tuple, int], logger: logging.Logger) -> int:
    """
    Pobiera service_id z cache lub z bazy danych. 
    Jeśli pociąg (numer, trasa, kategoria) istnieje, ale ma inną nazwę, aktualizuje ją.
    Jeśli nie istnieje, tworzy nowy wpis.
    """
    # Klucz cache uwzględnia docelową (poprawną) nazwę
    cache_key = (
        service_data['number'],
        service_data['name'],
        service_data['category_id'],
        service_data['is_domestic'],
        service_data['start_station_id'],
        service_data['end_station_id']
    )

    if cache_key in cache:
        return cache[cache_key]

    try:
        # Szukamy usługi po cechach unikalnych, ignorując (na chwilę) samą treść nazwy, 
        # aby uniknąć duplikatów wynikających z wielkości liter lub braku ogonków.
        search_query = {
            "number": service_data['number'],
            "category_id": service_data['category_id'],
            "start_station_id": service_data['start_station_id'],
            "end_station_id": service_data['end_station_id']
        }
        
        response = supabase.table('train_services').select('id, name').match(search_query).execute()

        if response.data:
            existing_service = response.data[0]
            service_id = existing_service['id']
            
            # Jeśli nazwa w bazie różni się od tej, którą chcemy (np. ALL CAPS vs Title Case), aktualizujemy ją.
            if existing_service['name'] != service_data['name']:
                logger.info(f"Aktualizacja nazwy pociągu {service_data['number']}: '{existing_service['name']}' -> '{service_data['name']}'")
                supabase.table('train_services').update({"name": service_data['name']}).eq('id', service_id).execute()
            
            cache[cache_key] = service_id
            return service_id

        # Jeśli nie znaleziono usługi — tworzymy nową
        insert_response = supabase.table('train_services').insert(service_data).execute()
        if insert_response.data:
            new_id = insert_response.data[0]['id']
            cache[cache_key] = new_id
            return new_id

    except Exception as e:
        logger.error(f"Błąd podczas pobierania/tworzenia service_id dla pociągu {service_data.get('number')}: {e}")
    return None

def _get_or_create_id(supabase: Client, table_name: str, column_name: str, value: Any, cache: Dict[Any, int],
                      logger: logging.Logger, new_stations: Set[str] = None) -> int:
    """
    Pobiera ID z cache'a lub tworzy nowy wpis w bazie danych, jeśli nie istnieje.
    Zwraca ID wpisu.
    """
    if table_name == 'stations' and isinstance(value, str):
        if value in STATION_NAME_OVERRIDES:
            value = STATION_NAME_OVERRIDES[value]
        else:
            norm_key = _normalize_station_key(value)
            if norm_key in STATION_NAME_OVERRIDES_NORMALIZED:
                value = STATION_NAME_OVERRIDES_NORMALIZED[norm_key]

    if value in cache:
        return cache[value]

    if not value or (isinstance(value, str) and not value.strip()):
        logger.warning(f"Próba zapisu pustej wartości do tabeli '{table_name}'. Pomijanie.")
        return None

    logger.info(f"Nowa wartość w tabeli '{table_name}': '{value}'. Dodawanie do bazy.")
    try:
        insert_data = {column_name: value}
        if table_name == 'stations':
            insert_data["is_domestic"] = True
            insert_data["passenger_volume_rank"] = None

        supabase.table(table_name).upsert(
            insert_data,
            on_conflict=column_name,
            ignore_duplicates=True
        ).execute()

        select_response = supabase.table(table_name).select("id").eq(column_name, value).single().execute()

        if select_response.data:
            new_id = select_response.data['id']
            cache[value] = new_id
            if table_name == 'stations' and new_stations is not None:
                new_stations.add(value)
            return new_id
        else:
            logger.error(f"Nie udało się pobrać ID dla wartości '{value}' w tabeli '{table_name}' po operacji upsert.")
            return None
    except Exception as e:
        logger.error(f"Błąd podczas operacji get_or_create dla tabeli '{table_name}': {e}")
        return None


import re

def _clean_difficulty_text(desc: str) -> Tuple[str, str]:
    """
    Czyści opis utrudnienia i wyodrębnia lokalizację (stację lub odcinek).
    Zwraca krotkę: (wyodrębniona_lokalizacja, oczyszczony_opis).
    """
    desc_clean = desc
    desc_clean = re.sub(r'\s*\(dot\.[^)]*?\)', '', desc_clean)
    desc_clean = re.sub(r'\s*/układ[^/]*?/', '', desc_clean)
    
    # Warunki pogodowe IMiGW
    if "IMiGW" in desc_clean:
        return None, "Trudne warunki atmosferyczne"
        
    # Podział na zdania
    text_normalized = re.sub(r'\s+', ' ', desc_clean).strip()
    parts = re.split(r'\.\s+', text_normalized)
    sentences = [p.strip() for p in parts if p.strip()]
    
    if len(sentences) >= 2:
        first = sentences[0]
        second = sentences[1]
        
        is_location = len(first) < 60 or " - " in first
        is_not_difficulty = not any(kw in first.lower() for kw in ["awaria", "usterka", "wypadek", "opóźnienia", "trudne", "złe", "pociąg", "kradzież"])
        
        if is_location and is_not_difficulty:
            return first, _map_difficulty_category(second)
        else:
            return None, _map_difficulty_category(first)
    else:
        return None, _map_difficulty_category(desc_clean)

def _map_difficulty_category(text: str) -> str:
    """
    Mapuje opis utrudnienia na znormalizowaną kategorię.
    """
    text_clean = re.sub(r'\s+', ' ', text).strip()
    text_lower = text_clean.lower()
    
    # 1. Warunki atmosferyczne
    if "warunki atmosferyczne" in text_lower or "ostrzeżeniami pogodowymi" in text_lower or "złe warunki" in text_lower or "trudne warunki" in text_lower:
        return "Trudne warunki atmosferyczne"
    if "drzewo na sieć" in text_lower or "drzewo na siec" in text_lower or "przewróconym drzewem" in text_lower:
        return "Przewrócone drzewo na sieć trakcyjną"
        
    # 2. Sterowanie ruchem i łączność
    if "sterowania ruchem" in text_lower or "urządzeń sterowania" in text_lower or "usterka urządzeń sterowania" in text_lower:
        return "Awaria urządzeń sterowania ruchem kolejowym"
    if "usterka systemu łączności" in text_lower:
        return "Usterka systemu łączności"
    if "awaria systemu informatycznego" in text_lower:
        return "Awaria systemu informatycznego"
        
    # 3. Trakcja i zasilanie
    if "awaria sieci trakcyjnej" in text_lower or "uszkodzona sieć trakcyjna" in text_lower or "usterka sieci trakcyjnej" in text_lower or "uszkodzona sieć" in text_lower or "oblodzona sieć" in text_lower:
        return "Awaria sieci trakcyjnej"
    if "brak zasilania" in text_lower:
        return "Brak zasilania sieci trakcyjnej"
    if "awaria elementów infrastruktury" in text_lower or "inne przyczyny związane z infrastrukturą" in text_lower or "awaria infrastruktury" in text_lower or "przyczyny związane z infrastrukturą" in text_lower:
        return "Awaria elementów infrastruktury kolejowej"
    if "kradzież elementów" in text_lower or "kradzieże i dewastacje" in text_lower or "kradzież element" in text_lower:
        return "Kradzież elementów infrastruktury kolejowej"
    if "awaria urządzeń energetycznych" in text_lower:
        return "Awaria urządzeń energetycznych"
        
    # 4. Tabor i pociągi
    if "awaria taboru" in text_lower or "defekt taboru" in text_lower or "awaria/uszkodzenie taboru" in text_lower or "naprawa pociągu" in text_lower or "uszkodzony pantograf" in text_lower or "defekt pociągu" in text_lower or "awaria pociągu" in text_lower or "uszkodzenie taboru" in text_lower or ("awaria" in text_lower and "pociąg" in text_lower):
        return "Awaria taboru"
    if "sprawdzenie stanu technicznego taboru" in text_lower:
        return "Sprawdzenie stanu technicznego taboru"
    if "włączanie/wyłączanie wagonów" in text_lower:
        return "Włączanie/wyłączanie wagonów"
        
    # 5. Wypadki i zdarzenia
    if "wypadek z udziałem człowieka" in text_lower or "wypadek z człowiekiem" in text_lower or "wydarzenie z udziałem człowieka" in text_lower:
        return "Wypadek z udziałem człowieka"
    if "wypadek z udziałem pojazdów" in text_lower or "wypadek z udziałem pojazdu" in text_lower or "udziałem samochodu" in text_lower:
        return "Wypadek z udziałem pojazdów drogowych"
    if "wypadek z udziałem zwierząt" in text_lower or "wypadek z udziałem zwierzyny" in text_lower or "kolizja ze zwierzętami" in text_lower or "wypadek z udziałem zwierzęcia" in text_lower or "wypadek ze zwierzętami" in text_lower or "kolizja ze zwierzęciem" in text_lower or "zderzenie ze zwierzęciem" in text_lower or "zderzenie ze zwierzętami" in text_lower:
        return "Kolizja ze zwierzętami"
    if "wypadek powodujący przerwę" in text_lower:
        return "Wypadek powodujący przerwę w ruchu pociągów"
        
    # 6. Przyczyny operacyjne, inwestycje i inne
    if "realizacją inwestycji" in text_lower or "prac modernizacyjnych" in text_lower:
        return "Przyczyny związane z realizacją inwestycji"
    if "nieprzewidziane wydarzenia" in text_lower or "nieprzewidziane zdarzenie" in text_lower or "nieprzewidziane wypadki" in text_lower or "nieprzewidziane wydarzenie" in text_lower:
        return "Inne"
    if "związane z utrzymaniem linii" in text_lower:
        return "Inne przyczyny związane z utrzymaniem linii kolejowych"
    if "opóźnienie z winy innego zarządcy" in text_lower or "utrudnienia w ruchu pociągów po stronie" in text_lower or "z winy innego zarządcy" in text_lower:
        return "Opóźnienie z winy innego zarządcy infrastruktury"
    if "interwencja służb porządkowych" in text_lower:
        return "Interwencja służb porządkowych"
    if "interwencja służb medycznych" in text_lower:
        return "Interwencja służb medycznych"
    if "interwencja służb ratowniczych" in text_lower:
        return "Interwencja służb ratowniczych"
    if "wydłużone przygotowanie wagonów" in text_lower:
        return "Wydłużone przygotowanie wagonów do drogi"
    if "wydłużone lokowanie" in text_lower:
        return "Wydłużone lokowanie pasażerów"
    if "wydłużone oczekiwanie" in text_lower:
        return "Wydłużone oczekiwanie na obsługę"
    if "odwołany" in text_lower or "został odwołany" in text_lower:
        return "Pociąg odwołany"
    if "z przyczyn technicznych" in text_lower and "opóźnienia" in text_lower:
        return "Inne"
    if "zdarzenie z pociągiem" in text_lower or "zdarzenie związane z prowadzeniem ruchu" in text_lower:
        return "Zdarzenie związane z prowadzeniem ruchu kolejowego"
    if "mogą wystąpić opóźnienia" in text_lower or "może wystąpić opóźnienie" in text_lower:
        return "Inne"
    if "wzajemne honorowanie biletów" in text_lower or "wzajemne honorowania biletów" in text_lower or "honorowanie biletów" in text_lower:
        return "Inne"
        
        
    clean_text = text_clean
    if clean_text.endswith("."):
        clean_text = clean_text[:-1].strip()
    return clean_text

def _parse_difficulty(info_array: List[str]) -> Tuple[str, str]:
    """
    Parsuje niespójne pole difficulties_info na opis i lokalizację.
    """
    description, location = None, None
    if not info_array or not info_array[0] or not info_array[0].strip():
        return None, None

    desc_part = info_array[0].strip()
    separators = ['##', '%']
    separator_found = None

    for sep in separators:
        if sep in desc_part:
            separator_found = sep
            break

    if separator_found:
        parts = desc_part.split(separator_found, 1)
        description = parts[0].strip() if parts[0].strip() else None
        location = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
    else:
        description = desc_part if desc_part else None

    if len(info_array) > 1 and info_array[1] and info_array[1].strip():
        location = info_array[1].strip()

    if location:
        location = location.lstrip('#').strip()
        if not location:
            location = None

    # Zastosowanie algorytmu czyszczenia i ekstrakcji lokalizacji
    if description:
        extracted_loc, cleaned_desc = _clean_difficulty_text(description)
        description = cleaned_desc
        if extracted_loc and not location:
            location = extracted_loc

    return description, location


def normalize_time(t: str) -> str:
    """Sprowadza format czasu (np. '14:35:00' lub '14:35') do spójnego formatu 'HH:MM'."""
    if not t:
        return None
    t_str = str(t).strip()
    if len(t_str) > 5 and t_str[2] == ':' and t_str[5] == ':':
        return t_str[:5]
    return t_str

def normalize_distance(d) -> float:
    """Zaokrągla dystans do dwóch miejsc po przecinku w celu uniknięcia problemów z dokładnością float."""
    if d is None:
        return 0.0
    try:
        return round(float(d), 2)
    except (ValueError, TypeError):
        return 0.0

def save_data(data_with_delays: list, logger: logging.Logger, update_occupancy: bool = False, overwrite: bool = False):
    """
    Zapisuje przetworzone dane pociągów do bazy danych PostgreSQL (Supabase),
    uwzględniając znormalizowany schemat i obsługę błędów.
    """
    logger.info("Rozpoczęto proces zapisywania danych do bazy danych.")

    try:
        url: str = os.environ.get("SUPABASE_URL")
        key: str = os.environ.get("SUPABASE_SERVICE_KEY")

        if not url or not key:
            logger.critical("Brak zmiennych środowiskowych SUPABASE_URL lub SUPABASE_SERVICE_KEY. Przerwano zapis.")
            return

        supabase: Client = create_client(url, key)
        logger.info("Pomyślnie połączono z bazą danych.")

        logger.info("Wczytywanie istniejących danych słownikowych do cache'a...")
        stations_cache = {s['name']: s['id'] for s in supabase.table('stations').select('id, name').execute().data}
        categories_cache = {c['category_code']: c['id'] for c in
                            supabase.table('train_categories').select('id, category_code').execute().data}
        occupancies_cache = {o['status_description']: o['id'] for o in
                             supabase.table('occupancies').select('id, status_description').execute().data}
        difficulties_cache = {d['description']: d['id'] for d in
                              supabase.table('difficulties').select('id, description').execute().data}

        # Cache dla usług (pociągów) - kluczem jest krotka cech
        services_data = supabase.table('train_services').select('id, number, name, category_id, is_domestic, start_station_id, end_station_id').execute().data
        services_cache = {
            (s['number'], s['name'], s['category_id'], s['is_domestic'], s['start_station_id'], s['end_station_id']): s['id']
            for s in services_data
        }

        logger.info(
            f"Wczytano: {len(stations_cache)} stacji, {len(categories_cache)} kategorii, {len(services_cache)} usług (pociągów).")

    except Exception as e:
        logger.critical(f"Krytyczny błąd podczas inicjalizacji połączenia lub cache'a: {e}")
        return

    runs_inserted, runs_skipped, runs_with_errors = 0, 0, 0
    stops_inserted = 0
    difficulties_links_inserted = 0
    new_stations: Set[str] = set()  # stacje odkryte po raz pierwszy w tej sesji

    for train_data in data_with_delays:
        train_number = train_data.get("number")

        if train_data.get("name", "").startswith("ZKA"):
            logger.info(f"Pociąg nr {train_number} ({train_data.get('name')}) pominięty (ZKA).")
            runs_skipped += 1
            continue

        try:
            category_id = _get_or_create_id(supabase, 'train_categories', 'category_code', train_data.get("category"),
                                            categories_cache, logger, None)
            start_station_id = _get_or_create_id(supabase, 'stations', 'name', train_data.get("from"), stations_cache,
                                                 logger, new_stations)
            end_station_id = _get_or_create_id(supabase, 'stations', 'name', train_data.get("to"), stations_cache,
                                               logger, new_stations)
            
            occupancy_id = None
            if train_data.get("occupancy"):
                occupancy_id = _get_or_create_id(supabase, 'occupancies', 'status_description', train_data.get("occupancy"),
                                                 occupancies_cache, logger, None)

            if not all([category_id, start_station_id, end_station_id]):
                logger.error(
                    f"Pociąg nr {train_number}: Nie udało się uzyskać ID dla jednej z kluczowych relacji (kategoria/stacje). Pomijanie.")
                runs_with_errors += 1
                continue

            # Obsługa nazwy pociągu (overrides i formatowanie)
            train_name_raw = train_data.get("name", "").strip()
            if train_name_raw in TRAIN_NAME_OVERRIDES:
                train_name = TRAIN_NAME_OVERRIDES[train_name_raw]
            elif "-" in train_name_raw:
                # Jeśli nazwa zawiera '-', traktujemy ją jako techniczny placeholder relacji
                # Zostawiamy FULL CAPS, aby frontend mógł to odfiltrować
                train_name = train_name_raw
            else:
                # Pozostałe nazwy własne formatujemy do Title Case (np. Albatros)
                train_name = train_name_raw.title()

            # Pobranie/Utworzenie service_id
            service_data = {
                "number": train_number,
                "name": train_name,
                "category_id": category_id,
                "is_domestic": train_data.get("domestic") == "Krajowy",
                "start_station_id": start_station_id,
                "end_station_id": end_station_id
            }
            service_id = _get_or_create_service_id(supabase, service_data, services_cache, logger)

            if not service_id:
                logger.error(f"Pociąg nr {train_number}: Nie udało się uzyskać service_id. Pomijanie.")
                runs_with_errors += 1
                continue

            run_to_insert = {
                "service_id": service_id,
                "date": train_data.get("date"),
                "occupancy_id": occupancy_id,
                "is_cancelled": train_data.get("is_cancelled", False)
            }

            response = supabase.table("train_runs").upsert(
                run_to_insert,
                on_conflict="service_id,date",
                ignore_duplicates=not (update_occupancy or overwrite)
            ).execute()

            inserted_run_id = None
            if response.data:
                inserted_run_id = response.data[0]['id']
            else:
                existing_run = supabase.table("train_runs").select("id").eq("service_id", service_id).eq("date", train_data.get("date")).execute()
                if existing_run.data:
                    inserted_run_id = existing_run.data[0]['id']

            if not inserted_run_id:
                logger.error(
                    f"Pociąg nr {train_number} z dnia {train_data.get('date')}: Nie udało się uzyskać ani utworzyć przejazdu. Pomijanie.")
                runs_with_errors += 1
                continue

            # Sprawdzamy, czy ten przejazd ma już przypisane przystanki
            existing_stops_res = supabase.table("run_stops").select("*").eq("run_id", inserted_run_id).order("stop_order").execute()
            existing_stops = existing_stops_res.data

            delay_info = train_data.get("delay_info")

            if existing_stops and not overwrite:
                logger.info(
                    f"Pociąg nr {train_number} z dnia {train_data.get('date')} już istnieje i ma zapisane przystanki. Pomijanie.")
                runs_skipped += 1
                continue

            if not isinstance(delay_info, list):
                logger.warning(
                    f"Pociąg nr {train_number}: Brak szczegółowych danych o trasie (delay_info = '{delay_info}'). Pomijanie aktualizacji trasy.")
                continue

            def get_station_id_from_cache(name: str) -> int:
                if not name:
                    return None
                val = name
                if val in STATION_NAME_OVERRIDES:
                    val = STATION_NAME_OVERRIDES[val]
                else:
                    norm_key = _normalize_station_key(val)
                    if norm_key in STATION_NAME_OVERRIDES_NORMALIZED:
                        val = STATION_NAME_OVERRIDES_NORMALIZED[norm_key]
                return stations_cache.get(val)

            # Porównywanie danych, jeśli overwrite jest włączone i istnieją przystanki w bazie
            is_data_identical = True
            if overwrite and existing_stops:
                # 1. Porównujemy status anulowania i frekwencję przejazdu
                existing_run_res = supabase.table("train_runs").select("is_cancelled, occupancy_id").eq("id", inserted_run_id).single().execute()
                existing_run_db = existing_run_res.data
                if existing_run_db:
                    db_cancelled = bool(existing_run_db.get("is_cancelled", False))
                    scr_cancelled = bool(train_data.get("is_cancelled", False))
                    db_occupancy = existing_run_db.get("occupancy_id")
                    scr_occupancy = occupancy_id
                    if db_cancelled != scr_cancelled or db_occupancy != scr_occupancy:
                        is_data_identical = False
                else:
                    is_data_identical = False

                # 2. Porównujemy liczbę przystanków
                if is_data_identical and len(existing_stops) != len(delay_info):
                    is_data_identical = False

                # 3. Szczegółowe porównanie każdego przystanku i utrudnień
                if is_data_identical:
                    # Pobieramy utrudnienia dla starych przystanków
                    existing_stop_ids = [s['id'] for s in existing_stops]
                    existing_diffs = []
                    if existing_stop_ids:
                        existing_diffs = supabase.table("run_stop_difficulties").select("stop_id, difficulty_id, location").in_("stop_id", existing_stop_ids).execute().data
                    
                    db_diffs_by_stop = {}
                    for d in existing_diffs:
                        db_diffs_by_stop.setdefault(d['stop_id'], []).append(d)

                    lagged_distance = 0.0
                    for i, stop_data in enumerate(delay_info):
                        db_stop = existing_stops[i]
                        
                        current_distance = lagged_distance
                        next_segment = stop_data.get("distance_km_from_start_to_next")
                        if isinstance(next_segment, (int, float)):
                            lagged_distance = next_segment

                        db_arr = normalize_time(db_stop.get("scheduled_arrival"))
                        scr_arr = normalize_time(stop_data.get("arrival_time"))
                        db_dep = normalize_time(db_stop.get("scheduled_departure"))
                        scr_dep = normalize_time(stop_data.get("departure_time"))
                        db_arr_delay = db_stop.get("delay_arrival_min")
                        scr_arr_delay = stop_data.get("delay_minutes_arrival")
                        db_dep_delay = db_stop.get("delay_departure_min")
                        scr_dep_delay = stop_data.get("delay_minutes_departure")
                        
                        db_station_id = db_stop.get("station_id")
                        scr_station_id = get_station_id_from_cache(stop_data.get("station_name"))

                        if (db_station_id != scr_station_id or
                            db_stop.get("stop_order") != i + 1 or
                            db_arr != scr_arr or
                            db_dep != scr_dep or
                            db_arr_delay != scr_arr_delay or
                            db_dep_delay != scr_dep_delay or
                            normalize_distance(db_stop.get("distance_from_start_km")) != normalize_distance(current_distance) or
                            bool(db_stop.get("is_cancelled", False)) != bool(stop_data.get("is_cancelled", False))):
                            is_data_identical = False
                            break

                        # Porównanie utrudnień na danym przystanku
                        desc, loc = _parse_difficulty(stop_data.get("difficulties_info"))
                        db_diffs = db_diffs_by_stop.get(db_stop['id'], [])
                        scraped_has_diff = desc is not None
                        db_has_diff = len(db_diffs) > 0

                        if scraped_has_diff != db_has_diff:
                            is_data_identical = False
                            break

                        if scraped_has_diff:
                            scraped_diff_id = difficulties_cache.get(desc)
                            if not scraped_diff_id:
                                is_data_identical = False
                                break

                            match_found = False
                            for df in db_diffs:
                                loc_db = df.get('location') or ""
                                loc_scr = loc or ""
                                if df['difficulty_id'] == scraped_diff_id and loc_db.strip() == loc_scr.strip():
                                    match_found = True
                                    break
                            if not match_found:
                                is_data_identical = False
                                break
            else:
                # W przypadku gdy existing_stops jest puste, dane nie są identyczne (musimy wstawić)
                if not existing_stops:
                    is_data_identical = False

            if is_data_identical:
                logger.info(f"Pociąg nr {train_number} z dnia {train_data.get('date')}: Dane są identyczne. Pomijanie zapisu.")
                runs_skipped += 1
                continue
            else:
                if existing_stops:
                    logger.info(f"Pociąg nr {train_number} z dnia {train_data.get('date')}: Wykryto różnice. Nadpisywanie przystanków...")
                    existing_stop_ids = [s['id'] for s in existing_stops]
                    if existing_stop_ids:
                        supabase.table("run_stop_difficulties").delete().in_("stop_id", existing_stop_ids).execute()
                    supabase.table("run_stops").delete().eq("run_id", inserted_run_id).execute()

                    # Aktualizacja właściwości przejazdu
                    supabase.table("train_runs").update({
                        "is_cancelled": train_data.get("is_cancelled", False),
                        "occupancy_id": occupancy_id
                    }).eq("id", inserted_run_id).execute()
                else:
                    if update_occupancy:
                        logger.info(f"Pociąg nr {train_number} z dnia {train_data.get('date')} jest nowy lub brakowało przystanków. Wyszukiwanie/tworzenie...")
                runs_inserted += 1

            stops_to_insert = []
            lagged_distance = 0.0
            for i, stop_data in enumerate(delay_info):
                station_id = _get_or_create_id(supabase, 'stations', 'name', stop_data.get("station_name"),
                                               stations_cache, logger, new_stations)
                if not station_id:
                    logger.warning(
                        f"Pociąg nr {train_number}: Nie można znaleźć/utworzyć stacji '{stop_data.get('station_name')}'. Pomijanie przystanku.")
                    continue

                current_distance = lagged_distance
                next_segment = stop_data.get("distance_km_from_start_to_next")
                if isinstance(next_segment, (int, float)):
                    lagged_distance = next_segment

                stops_to_insert.append({
                    "run_id": inserted_run_id,
                    "station_id": station_id,
                    "stop_order": i + 1,
                    "scheduled_arrival": stop_data.get("arrival_time"),
                    "scheduled_departure": stop_data.get("departure_time"),
                    "delay_arrival_min": stop_data.get("delay_minutes_arrival"),
                    "delay_departure_min": stop_data.get("delay_minutes_departure"),
                    "distance_from_start_km": current_distance,
                    "is_cancelled": stop_data.get("is_cancelled", False)
                })

            if not stops_to_insert:
                continue

            stops_response = supabase.table("run_stops").insert(stops_to_insert).execute()
            inserted_stops = stops_response.data
            stops_inserted += len(inserted_stops)

            difficulties_to_insert = []
            for i, stop_data in enumerate(delay_info):
                if 'difficulties_info' not in stop_data or not inserted_stops:
                    continue

                if i >= len(inserted_stops):
                    logger.warning(
                        f"Pociąg nr {train_number}: Niezgodność liczby wstawionych przystanków z danymi wejściowymi. Pomijanie utrudnień.")
                    break

                description, location = _parse_difficulty(stop_data["difficulties_info"])

                if description:
                    difficulty_id = _get_or_create_id(supabase, 'difficulties', 'description', description,
                                                      difficulties_cache, logger)
                    if difficulty_id:
                        difficulties_to_insert.append({
                            "stop_id": inserted_stops[i]['id'],
                            "difficulty_id": difficulty_id,
                            "location": location
                        })

            if difficulties_to_insert:
                supabase.table("run_stop_difficulties").insert(difficulties_to_insert).execute()
                difficulties_links_inserted += len(difficulties_to_insert)

        except Exception as e:
            logger.error(f"Krytyczny błąd podczas zapisu danych dla pociągu nr {train_number}: {e}", exc_info=True)
            runs_with_errors += 1

    logger.info("=" * 30)
    logger.info("PODSUMOWANIE ZAPISU DO BAZY DANYCH")
    logger.info(f"Nowe przejazdy: {runs_inserted}")
    logger.info(f"Pominięte przejazdy (duplikaty lub ZKA): {runs_skipped}")
    logger.info(f"Przejazdy z błędami: {runs_with_errors}")
    logger.info(f"Wstawione przystanki: {stops_inserted}")
    logger.info(f"Dodane powiązania utrudnień: {difficulties_links_inserted}")
    if new_stations:
        logger.warning(f"NOWE STACJE ODKRYTE ({len(new_stations)}): {sorted(new_stations)}")
    logger.info("=" * 30)

    # Powiadomienie GitHub Issue jeśli odkryto nowe stacje
    if new_stations:
        _append_to_stations_json(sorted(new_stations), logger)
        _create_github_issue(sorted(new_stations), logger)

def _append_to_stations_json(new_stations: list, logger: logging.Logger):
    """
    Dopisuje nowo odkryte stacje krajowe na koniec misc/stations.json.
    Ten plik jest później commitowany przez GitHub Actions i serwowany jako
    statyczny plik przez GitHub Pages — stanowi źródło prawdy dla validStations.
    """
    from pathlib import Path
    stations_path = Path(__file__).parent / 'docs' / 'misc' / 'stations.json'

    if not stations_path.exists():
        logger.warning(f"Nie znaleziono {stations_path} — pomijam aktualizację listy stacji.")
        return

    try:
        existing = json.loads(stations_path.read_text(encoding='utf-8'))
        added = []
        for name in new_stations:
            if name not in existing:
                existing.append(name)
                added.append(name)

        if added:
            stations_path.write_text(
                json.dumps(existing, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
            logger.info(f"Dopisano {len(added)} stacji do misc/stations.json: {added}")
        else:
            logger.info("Wszystkie nowe stacje już istnieją w misc/stations.json.")
    except Exception as e:
        logger.error(f"Błąd podczas aktualizacji misc/stations.json: {e}")


def _create_github_issue(new_stations: list, logger: logging.Logger):
    """
    Tworzy GitHub Issue z listą nowych stacji.
    Wymaga zmiennej środowiskowej GITHUB_TOKEN oraz GITHUB_REPO (np. 'marekk13/PKP-Intercity-Train-Delay-Scraper').
    Powiadomienie push przychodzi automatycznie przez apkę GitHub Mobile.
    """
    token = os.environ.get("GITHUB_TOKEN")
    repo  = os.environ.get("GITHUB_REPO", "marekk13/PKP-Intercity-Train-Delay-Scraper")

    if not token:
        logger.warning("Brak GITHUB_TOKEN — nie można utworzyć GitHub Issue. Nowe stacje: %s", new_stations)
        return

    station_list = '\n'.join(f'- `{s}`' for s in new_stations)
    body = (
        f"Skrypt scrapera wykrył **{len(new_stations)}** nową/nowych stację/stacji "
        f"nieobecnych wcześniej w bazie danych.\n\n"
        f"{station_list}\n\n"
        f"**Działania:**\n"
        f"1. Sprawdź czy stacja powinna znaleźć się w `misc/stations.json`\n"
        f"2. Jeśli tak — dodaj ją i uruchom `misc/sort_stations.py`\n"
        f"3. Upewnij się, że `is_domestic` jest ustawione poprawnie w bazie"
    )

    payload = json.dumps({
        "title": f"[Scraper] Nowe stacje: {', '.join(new_stations[:3])}{'...' if len(new_stations) > 3 else ''}",
        "body": body,
        "labels": ["nowa stacja", "wymaga uwagi"]
    }).encode()

    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/issues",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            issue = json.loads(resp.read())
            logger.info(f"GitHub Issue #{issue['number']} utworzone: {issue['html_url']}")
    except Exception as e:
        logger.error(f"Błąd tworzenia GitHub Issue: {e}")