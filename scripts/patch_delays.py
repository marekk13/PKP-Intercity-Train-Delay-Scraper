import os
import sys
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from get_delays import get_delays
from save_to_postgres import save_data

def patch_delays_for_dates(dates: list[str], logger: logging.Logger, overwrite: bool = False):
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        logger.critical("Brak SUPABASE_URL lub SUPABASE_SERVICE_KEY w środowisku.")
        return

    supabase: Client = create_client(url, key)
    
    trains_to_scrape = []

    for date_str in dates:
        logger.info(f"Rozpoczęto analizę dla daty: {date_str} (overwrite={overwrite})")
        
        try:
            # 1. Pobierz wszystkie train_runs dla tej daty wraz z ich ID
            runs_response = supabase.table('train_runs').select('id, service_id, run_stops(id)').eq('date', date_str).execute()
            runs = runs_response.data
            
            # Fallback jeśli brak jakichkolwiek rekordów dla tej daty
            if not runs:
                logger.warning(f"Brak rekordów train_runs dla daty {date_str} w bazie. Fallback: pobieranie aktywnych usług z ostatnich 14 dni...")
                from datetime import timedelta
                parsed_d = datetime.strptime(date_str, "%Y-%m-%d").date()
                start_limit = (parsed_d - timedelta(days=7)).strftime("%Y-%m-%d")
                end_limit = (parsed_d + timedelta(days=7)).strftime("%Y-%m-%d")
                
                recent_runs = supabase.table('train_runs').select('service_id').gte('date', start_limit).lte('date', end_limit).execute()
                active_service_ids = list(set(r['service_id'] for r in recent_runs.data))
                
                if not active_service_ids:
                    logger.error(f"Nie znaleziono żadnych aktywnych usług w przedziale {start_limit} - {end_limit}. Pomijam datę.")
                    continue
                
                # Budujemy sztuczną listę runs do dalszego przetwarzania
                runs = [{"service_id": sid, "run_stops": []} for sid in active_service_ids]
            
            # 2. Wybierz pociągi do przetworzenia w zależności od trybu
            if overwrite:
                target_runs = runs
            else:
                target_runs = [r for r in runs if not r.get('run_stops')]
            
            if not target_runs:
                if overwrite:
                    logger.info(f"Brak pociągów do przetworzenia dla daty {date_str}.")
                else:
                    logger.info(f"Brak luk w dacie {date_str}. Wszystkie train_runs posiadają odpowiadające run_stops.")
                continue
                
            logger.info(f"Do przetworzenia w dacie {date_str}: {len(target_runs)} pociągów (overwrite={overwrite}).")
            
            service_ids = list(set(r['service_id'] for r in target_runs))
            
            # 3. Pobierz szczegóły usług dla przejazdów
            services_response = supabase.table('train_services').select('id, number, name, is_domestic, category_id, start_station_id, end_station_id').in_('id', service_ids).execute()
            services = {s['id']: s for s in services_response.data}
            
            # Pobierz dodatkowo słowniki dla czytelnych nazw
            categories_response = supabase.table('train_categories').select('id, category_code').execute()
            categories = {c['id']: c['category_code'] for c in categories_response.data}
            
            stations_response = supabase.table('stations').select('id, name').execute()
            stations = {s['id']: s['name'] for s in stations_response.data}
            
            # Format target_date z "YYYY-MM-DD" do "DD.MM.YYYY" wymagany przez portal pasażera
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
            target_date_formatted = parsed_date.strftime("%d.%m.%Y")
            
            # 4. Budowanie listy danych wejściowych
            for run in target_runs:
                service = services.get(run['service_id'])
                if not service:
                    continue
                    
                trains_to_scrape.append({
                    "number": service["number"],
                    "name": service["name"],
                    "category": categories.get(service["category_id"], ""),
                    "domestic": "Krajowy" if service["is_domestic"] else "Międzynarodowy",
                    "from": stations.get(service["start_station_id"], ""),
                    "to": stations.get(service["end_station_id"], ""),
                    "date": date_str,
                    "target_date": target_date_formatted
                })
        except Exception as e:
            logger.error(f"Błąd podczas odpytywania bazy danych dla daty {date_str}: {e}", exc_info=True)

    if not trains_to_scrape:
        logger.info("Brak pociągów do przetworzenia.")
        return

    logger.info(f"Łącznie pociągów do przetworzenia ze wszystkich dat: {len(trains_to_scrape)}")
    
    # 5. Uruchomienie scrapera
    scraped_data = get_delays(trains_to_scrape, logger=logger)
    
    # 6. Zapisanie uzyskanych opóźnień do bazy
    save_data(scraped_data, logger=logger, overwrite=overwrite)
    logger.info("Zakończono łatanie danych.")


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Skrypt łatający luki w opóźnieniach lub wczytujący dane z pliku.")
    parser.add_argument("--dates", nargs="+", help="Daty do sprawdzenia w formacie YYYY-MM-DD (np. 2026-06-08)")
    parser.add_argument("--yesterday", action="store_true", help="Uruchom dla wczorajszej daty")
    parser.add_argument("--overwrite", action="store_true", help="Nadpisz istniejące dane w bazie, jeśli są różnice")
    parser.add_argument("--file", help="Ścieżka do pliku JSON z danymi do wczytania i aktualizacji frekwencji")
    args = parser.parse_args()

    dates = []
    if args.dates:
        dates.extend(args.dates)
    if args.yesterday:
        from datetime import timedelta
        from zoneinfo import ZoneInfo
        warsaw_tz = ZoneInfo("Europe/Warsaw")
        yesterday_str = (datetime.now(warsaw_tz) - timedelta(days=1)).strftime("%Y-%m-%d")
        dates.append(yesterday_str)

    if not dates and not args.file:
        parser.error("Należy podać parametr --dates, --yesterday lub --file.")

    # Konfiguracja loggera specyficznego dla tego skryptu
    logger = logging.getLogger("patch_delays")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Handler dla konsoli (stdout)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Handler dla pliku logów (tylko poza GitHub Actions / CI)
    if os.environ.get("GITHUB_ACTIONS") != "true":
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"patch_delays_{timestamp}.log")

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if args.file:
        import json
        logger.info(f"Wczytywanie danych z pliku: {args.file}")
        try:
            with open(args.file, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
            logger.info(f"Pomyślnie wczytano {len(data)} rekordów. Rozpoczynanie zapisu i aktualizacji frekwencji.")
            save_data(data, logger=logger, update_occupancy=True, overwrite=args.overwrite)
            logger.info("Zakończono wczytywanie danych z pliku.")
        except Exception as e:
            logger.error(f"Błąd podczas wczytywania/zapisu danych z pliku: {e}", exc_info=True)
    else:
        patch_delays_for_dates(dates, logger, overwrite=args.overwrite)
