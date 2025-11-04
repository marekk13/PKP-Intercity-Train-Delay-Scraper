# PKP Intercity Train Data Scraper & Analysis Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This project is a robust, automated data engineering pipeline designed to scrape, process, and store real-time train delay and occupancy data from Polish national rail carrier PKP Intercity. The collected data is normalized and persisted in a PostgreSQL database, creating a historical dataset suitable for analysis, reporting, and further applications.

## The Problem

Official Polish railway carriers provide valuable public data, such as real-time train schedules, delays, and disruptions. However, this information is not exposed via a public API or offered in a machine-readable format. Access is often protected by dynamic web elements and complex session handling, making automated data collection challenging. This practice limits transparency and hinders the creation of value-added services, despite EU directives (like Directive 2019/1024) promoting open data.

This project bridges that gap by implementing a reliable, server-side pipeline that systematically gathers this public data and transforms it into a structured, queryable format.

## Key Features

-   **Fully Automated Execution**: Runs on a daily schedule using GitHub Actions, requiring no manual intervention.
-   **Robust Two-Stage Scraping**: Utilizes the modern **Playwright** framework to handle dynamic, JavaScript-heavy websites, ensuring reliability.
-   **Structured Data Persistence**: Archives data in a normalized PostgreSQL database, enabling complex queries and historical analysis, not just storing raw files.
-   **Data Normalization**: Implements a relational database schema to avoid data redundancy and ensure consistency (e.g., separate tables for stations, train categories, and disruptions).
-   **Comprehensive Logging**: Features a dedicated logging module that outputs to both the console and versioned log files for easy debugging and monitoring.
-   **Scalable and Maintainable**: The codebase is modular, with clear separation of concerns for data fetching, scraping, and database operations.
-   **Backup and Archiving**: In addition to the database, each run generates a timestamped JSON file as a raw data backup.

## System Architecture & Data Flow

The entire process is orchestrated by the main script and executed automatically within a GitHub Actions workflow.

1.  **Scheduled Trigger**: The `scraper.yml` workflow is triggered daily at a scheduled time.
2.  **Fetch Initial Train List (`get_train_data.py`)**: The pipeline begins by scraping `intercity.pl` to get a complete list of all trains running on the target day. This initial data includes train number, name, category, and route.
3.  **Scrape Detailed Delay Information (`get_delays.py`)**: For each train identified, a Playwright-controlled headless browser navigates to the `portalpasazera.pl` portal. It automates searching for the train by its number and meticulously parses its entire timeline to extract:
    *   Scheduled and delayed arrival/departure times for every station.
    *   Distance markers and travel time between stations.
    *   Information about any disruptions or difficulties on the route.
4.  **Persist Data to PostgreSQL (`save_to_postgres.py`)**: The processed data is then sent to a PostgreSQL database (via Supabase). This script handles:
    *   Connecting to the database using secure environment variables.
    *   Caching dictionary data (stations, categories) to minimize DB queries.
    *   Normalizing the data by inserting or updating records across multiple tables (`train_runs`, `run_stops`, `stations`, `difficulties`, etc.).
5.  **Generate JSON Backup**: A complete JSON dump of the session's scraped data is saved to the `data/` directory with a unique timestamp, serving as a persistent backup.
6.  **Logging**: Throughout the process, events, warnings, and errors are logged to a timestamped file in the `logs/` directory.

## Tech Stack

-   **Language**: Python 3.x
-   **Browser Automation**: **Playwright** for robustly handling modern, dynamic websites.
-   **Database**: PostgreSQL.
-   **Database Client**: **supabase-py** for interacting with the Supabase PostgreSQL backend.
-   **Automation/CI/CD**: **GitHub Actions** for scheduled execution.
-   **Infrastructure**: Deployed on a **self-hosted runner** (e.g., a cloud VM).

## Database Schema

The data is stored in a relational schema to ensure integrity and facilitate efficient querying. Below is a simplified overview of the main tables:

-   `train_runs`: The central table, holding one record for each unique train journey on a specific date.
    -   `id`, `number`, `name`, `date`, `category_id`, `start_station_id`, `end_station_id`, `occupancy_id`.
-   `run_stops`: Links a train run to all the stations on its route.
    -   `id`, `run_id`, `station_id`, `stop_order`, `scheduled_arrival`, `delay_arrival_min`, `distance_from_start_km`.
-   `stations`: A dictionary table for all unique station names.
    -   `id`, `name`.
-   `train_categories`: A dictionary table for train types (e.g., IC, EIP, TLK).
    -   `id`, `category_code`.
-   `difficulties`: A dictionary table for unique disruption reasons.
    -   `id`, `description`.
-   `run_stop_difficulties`: A link table connecting a specific stop on a run with a reported difficulty.
    -   `id`, `stop_id`, `difficulty_id`, `location`.

## Setup and Usage

### Prerequisites

-   Python 3.8+
-   A running PostgreSQL database.
-   Environment variables configured for database connection (see below).

### Installation

1.  **Clone the repository:**
```bash
git clone <your-repository-url>
cd <repository-directory>
```

2.  **Create and activate a virtual environment:**
```bash
# For macOS/Linux
python3 -m venv .venv
source .venv/bin/activate

# For Windows
python -m venv .venv
.venv\Scripts\activate
```

3.  **Install the required dependencies:**
```bash
pip install -r requirements.txt
```

4.  **Install Playwright browser dependencies:**
```bash
playwright install chromium
```

### Configuration

The script requires environment variables to connect to the Supabase/PostgreSQL database. Create a `.env` file or set them in your environment.

### Running the Scraper Manually

To execute the entire pipeline manually, run the main script:

```bash
python get_train_data.py
```

## Automation with GitHub Actions

This project is designed for automated execution using the provided workflow file.

**File:** `.github/workflows/scraper.yml`

```yaml
name: Scrape Train Delays

on:
  schedule:
    - cron: '00 22 * * *'
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