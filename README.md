# PKP Intercity Train Data Scraper & Analysis Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This project is a comprehensive data engineering pipeline and web application designed to scrape, process, and serve real-time train delay and occupancy data from the Polish national rail carrier, PKP Intercity. The pipeline normalizes the collected data into a PostgreSQL database, exposing it through a public REST API and a user-friendly frontend interface ([spoznienia.me](https://spoznienia.me)), making it easily accessible for passengers and researchers alike.

## The Problem

Official Polish railway carriers provide valuable public data, such as real-time train schedules, delays, and disruptions. However, this information is not exposed via a public API or offered in a machine-readable format. Access is often protected by dynamic web elements and complex session handling, making automated data collection challenging. This practice limits transparency and hinders the creation of value-added services, despite EU directives (like Directive 2019/1024) promoting open data.

This project bridges that gap by implementing a reliable, server-side pipeline that systematically gathers this public data and transforms it into a structured, queryable format.

## Key Features

- **Public API**: Features a fast, rate-limited REST API built with FastAPI, providing programmatic access to historical and real-time schedules, delays, and station boards.
- **Web Interface**: Powering the [spoznienia.me](https://spoznienia.me) frontend, allowing users to easily check train delays and station schedules in a modern, mobile-friendly UI.
- **Fully Automated Execution**: Scraper runs on a daily schedule using GitHub Actions, requiring no manual intervention.
- **Robust Data Scraping**: Utilizes **Playwright** to handle dynamic, JavaScript-heavy websites, ensuring reliable data extraction.
- **Structured Data Persistence**: Archives data in a normalized PostgreSQL database (Supabase), enabling complex queries and historical analysis.
- **Data Normalization**: Implements an relational schema (e.g., separate tables for services, runs, stations, categories, and disruptions) to avoid redundancy and maintain data integrity.

## System Architecture & Data Flow

The entire process is orchestrated by the main script and executed automatically within a GitHub Actions workflow.

1.  **Scheduled Trigger**: The `scraper.yml` workflow is triggered daily at a scheduled time.
2.  **Fetch Initial Train List (`get_train_data.py`)**: The pipeline begins by scraping `intercity.pl` to get a complete list of all trains running on the target day. This initial data includes train number, name, category, and route.
3.  **Scrape Detailed Delay Information (`get_delays.py`)**: For each train identified, a Playwright-controlled headless browser navigates to the `portalpasazera.pl` portal. It automates searching for the train by its number and meticulously parses its entire timeline to extract:
    - Scheduled and delayed arrival/departure times for every station.
    - Distance markers and travel time between stations.
    - Information about any disruptions or difficulties on the route.
4.  **Persist Data to PostgreSQL (`save_to_postgres.py`)**: The processed data is then sent to a PostgreSQL database (via Supabase). This script handles:
    - Connecting to the database using secure environment variables.
    - Caching dictionary data (stations, categories) to minimize DB queries.
    - Normalizing the data by inserting or updating records across multiple tables (`train_runs`, `run_stops`, `stations`, `difficulties`, etc.).
5.  **Generate JSON Backup**: A complete JSON dump of the session's scraped data is saved to the `data/` directory with a unique timestamp, serving as a persistent backup.
6.  **REST API**: A FastAPI backend (hosted on Render) connects to the database and serves the scraped data to the public and the frontend application, using aggressive caching and rate limiting for performance.
7.  **Frontend**: A web UI hosted at [spoznienia.me](https://spoznienia.me) consumes the API to display data to users.

## Tech Stack

- **Backend/API**: Python 3.x, **FastAPI**, Uvicorn, SlowAPI (rate limiting), FastAPI-Cache. Hosted on **Render**.
- **Browser Automation**: **Playwright** for robustly handling modern, dynamic websites.
- **Database**: PostgreSQL hosted on **Supabase** (interacted with via `supabase-py`).
- **Automation/CI/CD**: **GitHub Actions** for scheduled execution.
- **Frontend**: Consumed by a modern UI hosted on GitHub Pages / [spoznienia.me](https://spoznienia.me).

## Database Schema

The data is stored in a normalized relational schema to ensure integrity and facilitate efficient querying. Below is a simplified overview of the main tables:

- `train_services`: Defines a specific train route and its static properties.
  - `id`, `number`, `name`, `category_id`, `is_domestic`, `start_station_id`, `end_station_id`.
- `train_runs`: Holds one record for a specific instance of a train service on a given date.
  - `id`, `service_id`, `date`, `occupancy_id`.
- `run_stops`: Links a train run to all the stations on its route, storing schedule and delay info.
  - `id`, `run_id`, `station_id`, `stop_order`, `scheduled_arrival`, `scheduled_departure`, `delay_arrival_min`, `delay_departure_min`, `distance_from_start_km`.
- `stations`: Dictionary table for all unique station names.
  - `id`, `name`, `is_domestic`, `passenger_volume_rank`.
- `occupancies`, `train_categories`, `difficulties`: Dictionary tables for occupancy levels, train types (e.g., IC, EIP), and disruption descriptions.
- `run_stop_difficulties`: A link table connecting a specific stop on a run with a reported difficulty.
  - `id`, `stop_id`, `difficulty_id`, `location`.

## Public API Usage

The project exposes a RESTful API hosted on Render, offering endpoints to query the collected data. The API is rate-limited and heavily cached.

### Endpoints

- `GET /stations`: Returns a list of all supported domestic station names ranked by passenger volume.
- `GET /train-runs`: Returns a list of train summaries (filtered by date, train number, or specific station).
- `GET /stations/{name}/schedule`: Get the departures/arrivals board for a specific station on a specific date.
- `GET /train-runs/{train_id}`: Get the full detail of a specific train run, including timeline, delays at each stop, and reported difficulties.

## Local Development (Optional)

If you wish to run the scraper or the API locally:

1.  **Clone the repository and prepare the environment:**

It is recommended to use [uv](https://github.com/astral-sh/uv) for fast and reliable dependency management.

```bash
git clone https://github.com/marekk13/PKP-Intercity-Train-Delay-Scraper.git
cd PKP-Intercity-Train-Delay-Scraper

# Create virtual environment and install dependencies
uv sync

# Install Playwright browser
uv run playwright install chromium
```

2.  **Configuration:** Create a `.env` file with `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`.

3.  **Run the API:**

```bash
uv run uvicorn api.main:app --reload
```

4.  **Run the Scraper manually:**

```bash
uv run python get_train_data.py
```

## Legacy Installation (pip)

If you don't have `uv` installed, you can still use standard `pip`:

```bash
python -m venv .venv
source .venv/bin/activate # (.venv\Scripts\activate on Windows)
pip install -r requirements.txt
playwright install chromium
```

## Automation with GitHub Actions

This project is designed for automated execution using the provided workflow file.

**File:** `.github/workflows/scraper.yml`

```yaml
name: Scrape Train Delays

on:
  schedule:
    - cron: "00 22 * * *"
  workflow_dispatch:

jobs:
  scrape:
    runs-on: self-hosted
    env:
      SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
      SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
    steps:
      - name: Check out repository
        uses: actions/checkout@v4
        with:
          clean: false # Prevents deletion of old data files

      - name: Install dependencies and run scraper
        # ... (steps to setup venv and run the script)
```

The workflow is configured to run on a **self-hosted runner**, which you must set up and connect to your GitHub repository. The `clean: false` option is used to ensure that historical JSON backups are preserved across runs.

## Output Structure

### 1. Primary Output: PostgreSQL Database

The most valuable output is the structured data populated in the PostgreSQL database, as described in the **Database Schema** section.

### 2. Backup: JSON File

For each run, a backup file is generated in the `data/` directory with a unique name like `train_data_YYYY-MM-DD-HHMM.json`.

**Example `train_data.json` entry:**

```json
[
  {
    "domestic": "Krajowy",
    "number": "5322",
    "category": "IC",
    "name": "MAZURY",
    "from": "Olsztyn Główny",
    "to": "Łódź Fabryczna",
    "occupancy": "Szacowana frekwencja poniżej 50%",
    "date": "2025-11-04",
    "delay_info": [
      {
        "station_name": "Olsztyn Główny",
        "arrival_time": null,
        "departure_time": "09:40",
        "delay_minutes_arrival": null,
        "delay_minutes_departure": 0,
        "distance_km_from_start_to_next": 12.3,
        "travel_time_from_start_to_next": "0h:10min",
        "difficulties_info": ["", ""]
      },
      {
        "station_name": "Olsztyn Zachodni",
        "arrival_time": "09:44",
        "departure_time": "09:45",
        "delay_minutes_arrival": 0,
        "delay_minutes_departure": 0,
        "distance_km_from_start_to_next": 25.8,
        "travel_time_from_start_to_next": "0h:15min",
        "difficulties_info": ["", ""]
      }
    ]
  }
]
```

### 3. Log Files

A detailed log file is created for each run in the `logs/` directory (e.g., `scraper_log_2025-11-04-2310.log`), capturing all operational events, warnings, and errors.

## Disclaimer

This tool is intended for educational and data analysis purposes. It scrapes data from publicly accessible websites. Please use this script responsibly and be mindful of the websites' terms of service. The author is not responsible for any misuse of this tool.

## License

This project is licensed under the MIT License.
