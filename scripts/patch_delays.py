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

def patch_delays_for_dates(dates: list[str], logger: logging.Logger):
    load_dotenv()
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        logger.critical("Brak SUPABASE_URL lub SUPABASE_SERVICE_KEY w środowisku.")
        return

    supabase: Client = create_client(url, key)
    
    trains_to_scrape = []

    for date_str in dates:
        logger.info(f"Pobieranie brakujących danych dla daty: {date_str}")
        
        try:
            # 1. Pobierz wszystkie train_runs dla tej daty wraz z ich ID
            runs_response = supabase.table('train_runs').select('id, service_id, run_stops(id)').eq('date', date_str).execute()
            runs = runs_response.data
            
            if not runs:
                logger.info(f"Brak rekordów train_runs dla daty {date_str}. Pomiń.")
                continue
                
            # 2. Odfiltruj te, które nie mają żadnego przypisanego rekordu w run_stops
            missing_runs = [r for r in runs if not r.get('run_stops')]
            
            if not missing_runs:
                logger.info(f"Brak luk w dacie {date_str}. Wszystkie train_runs posiadają odpowiadające run_stops.")
                continue
                
            logger.info(f"Znaleziono {len(missing_runs)} pociągów bez rekordów run_stops z dnia {date_str}.")
            
            service_ids = list(set(r['service_id'] for r in missing_runs))
            
            # 3. Pobierz szczegóły usług dla brakujących przejazdów
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
            
            # 4. Budowanie listy danych wejściowych (zgodnie z formatem wejściowym get_delays)
            for run in missing_runs:
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
                    "target_date": target_date_formatted # Nowe pole do kalendarza
                })
        except Exception as e:
            logger.error(f"Błąd podczas odpytywania bazy danych dla daty {date_str}: {e}")

    if not trains_to_scrape:
        logger.info("Brak pociągów do przetworzenia.")
        return

    logger.info(f"Łącznie pociągów do przetworzenia: {len(trains_to_scrape)}")
    
    # 5. Uruchomienie scrapera
    scraped_data = get_delays(trains_to_scrape, logger=logger)
    
    # 6. Zapisanie uzyskanych opóźnień do bazy
    save_data(scraped_data, logger=logger)
    logger.info("Zakończono łatanie danych.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skrypt łatający luki w opóźnieniach.")
    parser.add_argument("--dates", nargs="+", required=True, help="Daty do sprawdzenia w formacie YYYY-MM-DD (np. 2026-06-08)")
    args = parser.parse_args()

    # Konfiguracja loggera specyficznego dla tego skryptu
    logger = logging.getLogger("patch_delays")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Handler dla konsoli (stdout)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Handler dla pliku logów
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"patch_delays_{timestamp}.log")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    patch_delays_for_dates(args.dates, logger)
