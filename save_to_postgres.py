import os
import json
import logging
import urllib.request
from supabase import create_client, Client
from typing import Dict, List, Any, Tuple, Set

STATION_NAME_OVERRIDES = {
    "Kraków Główny Osobowy": "Kraków Główny",
    "Gdynia Główna Osobowa": "Gdynia Główna",
    "Lublin": "Lublin Główny",
    "Rzeszów": "Rzeszów Główny",
    "Zielona Góra": "Zielona Góra Główna",
    "Radom": "Radom Główny",
    "Kielce": "Kielce Główne",
    "Krynica": "Krynica-Zdrój",
    "Bielsko Biała Główna": "Bielsko-Biała Główna",
    "Kudowa Zdrój": "Kudowa-Zdrój",
    "Polanica Zdrój": "Polanica-Zdrój",
    "Olsztyn Główny/Szczecin Główny": "Olsztyn Główny",
    "Szczecin Główny/Świnoujście": "Szczecin Główny",

    "Berlin Hauptbahn": "Berlin Hauptbahnhof",
    "Berlin-Charlotte": "Berlin-Charlottenburg",
    "Berlin Zoolog Garten": "Berlin Zoologischer Garten",
    "Leipzig Hbf": "Leipzig Hauptbahnhof",
    "Muenchen Hbf": "München Hauptbahnhof",
    "Muenchen Ost": "München Ost",
    "Graz Hbf": "Graz Hauptbahnhof",
    "Muerzzuschlag": "Mürzzuschlag",
    "Salzburg Hbf.": "Salzburg Hauptbahnhof",
    "St.Poelten Hbf": "St. Pölten Hauptbahnhof",
    "Wien Hbf": "Wien Hauptbahnhof",
    "Wien Westbf": "Wien Westbahnhof",

    "Dnipro": "Dnipro-Holovnyi",
    "Dnipro Hl.": "Dnipro-Holovnyi",
    "Harkov-Passajirs": "Kharkiv-Pasazhyrskyi",
    "Charków": "Kharkiv-Pasazhyrskyi",
    "Kiev-Passajirski": "Kyiv-Pasazhyrskyi",
    "Kyiv-Pas.": "Kyiv-Pasazhyrskyi",
    "Kijów": "Kyiv-Pasazhyrskyi",
    "Odessa-Glavnaia": "Odesa-Holovna",
    "Zaporoze": "Zaporizhzhia-1",
    "Zaporoże": "Zaporizhzhia-1",
    "Krzywy Róg": "Kryvyi Rih",
    
    "Bratislava Hlavna Stanica": "Bratislava hlavná stanica",
    "Bohumin": "Bohumín",
    "Breclav": "Břeclav",
    "Ceska Trebova": "Česká Třebová",
    "Hodonin": "Hodonín",
    "Hranice na Morave": "Hranice na Moravě",
    "Jablonne nad Orlici": "Jablonné nad Orlicí",
    "Nove Zamky": "Nové Zámky",
    "Olomouc Hlavni Nadrazi": "Olomouc hlavní nádraží",
    "Ostrava Hlavni Nadrazi": "Ostrava hlavní nádraží",
    "Pardubice Hlavni Nadrazi": "Pardubice hlavní nádraží",
    "Praha Hlavni Nadrazi": "Praha hlavní nádraží",
    "Praha": "Praha hlavní nádraží",
    "Praha hl.n.": "Praha hlavní nádraží",
    "Praha-Liben": "Praha-Libeň",
    "Praha-Vrsovice": "Praha-Vršovice",
    "Prerov": "Přerov",
    "Stare Mesto u Uher. Hradiste": "Staré Město u Uherského Hradiště",
    "Zabreh na Morave": "Zábřeh na Moravě",
    "Usti nad Orlici": "Ústí nad Orlicí",
    "Usti nad Orlici Mesto": "Ústí nad Orlicí město",
    "Sturovo": "Štúrovo",

    # Litwa
    "Wilnus": "Vilnius"
}

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
        logger.info(
            f"Wczytano: {len(stations_cache)} stacji, {len(categories_cache)} kategorii, {len(occupancies_cache)} statusów frekwencji, {len(difficulties_cache)} utrudnień.")

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

            run_to_insert = {
                "number": train_number,
                "name": train_data.get("name"),
                "is_domestic": train_data.get("domestic") == "Krajowy",
                "date": train_data.get("date"),
                "category_id": category_id,
                "start_station_id": start_station_id,
                "end_station_id": end_station_id,
                "occupancy_id": occupancy_id
            }

            response = supabase.table("train_runs").upsert(
                run_to_insert,
                on_conflict="number,date",
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
    stations_path = Path(__file__).parent / 'misc' / 'stations.json'

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