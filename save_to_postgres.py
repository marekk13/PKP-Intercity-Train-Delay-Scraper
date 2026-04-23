import os
import json
import logging
import urllib.request
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
    if table_name == 'stations' and isinstance(value, str) and value in STATION_NAME_OVERRIDES:
        value = STATION_NAME_OVERRIDES[value]

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

    return description, location


def save_data(data_with_delays: list, logger: logging.Logger):
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
            occupancy_id = _get_or_create_id(supabase, 'occupancies', 'status_description', train_data.get("occupancy"),
                                             occupancies_cache, logger, None)

            if not all([category_id, start_station_id, end_station_id, occupancy_id]):
                logger.error(
                    f"Pociąg nr {train_number}: Nie udało się uzyskać ID dla jednej z kluczowych relacji (kategoria/stacje/frekwencja). Pomijanie.")
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
                "occupancy_id": occupancy_id
            }

            response = supabase.table("train_runs").upsert(
                run_to_insert,
                on_conflict="service_id,date",
                ignore_duplicates=True
            ).execute()

            if not response.data:
                logger.info(
                    f"Pociąg nr {train_number} z dnia {train_data.get('date')} już istnieje w bazie lub wystąpił błąd. Pomijanie.")
                runs_skipped += 1
                continue

            inserted_run_id = response.data[0]['id']
            runs_inserted += 1

            delay_info = train_data.get("delay_info")
            if not isinstance(delay_info, list):
                logger.warning(
                    f"Pociąg nr {train_number}: Brak szczegółowych danych o trasie (delay_info = '{delay_info}'). Wstawiono tylko główny rekord przejazdu.")
                continue

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
                    "distance_from_start_km": current_distance
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