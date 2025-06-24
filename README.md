# PKP Intercity Train Delay Scraper

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This project is a Python-based web scraper designed to automatically collect real-time delay information for all PKP Intercity trains operating in Poland. It generates a structured JSON file containing detailed, station-by-station data for each train journey.

## The Problem

PKP Intercity, Poland's primary long-distance rail carrier, provides public data of significant interest, such as train schedules and real-time delays. However, this information is not made available through a public API or in a machine-readable format like JSON or CSV.

Furthermore, the main passenger portal (`portalpasazera.pl`) protects access to specific train data using dynamically generated tokens (`sid` and `pid`), making automated data collection challenging. This practice contradicts the principles of open data and the EU Directive 2019/1024 on open data and the re-use of public sector information.

This project aims to bridge that gap by providing a reliable method for collecting this public data through web scraping, simulating user behavior in a headless browser.

## Features

-   Fetches a daily list of all active PKP Intercity trains, including their number, route, and occupancy predictions.
-   For each train, it retrieves detailed, real-time delay data from the official passenger portal.
-   Uses **Selenium** with a headless browser to navigate the dynamic, JavaScript-heavy website.
-   Handles common website errors, such as non-existent train numbers or search result mismatches.
-   Implements comprehensive logging to both the console and a daily log file (`scraper_log_YYYY-MM-DD.log`).
-   Outputs the collected data into a clean, structured, and timestamped JSON file for easy analysis.

## How It Works

The data collection process is performed in two main steps, orchestrated by the main script `get_train_data.py`:

1.  **Fetching the Train List:**
    -   The script first sends a request to the PKP Intercity frequency page (`intercity.pl`).
    -   It parses the HTML table to get a list of all trains scheduled for the current day, including their numbers, categories, names, and routes.

2.  **Scraping Delay Details:**
    -   Next, the script launches a Selenium-controlled headless Chrome browser.
    -   For each train number obtained in the first step, it navigates to the Passenger Portal (`portalpasazera.pl`).
    -   It automates the following actions:
        -   Accepting the cookie consent banner.
        -   Switching the search mode to "Search by number".
        -   Entering the train number and clicking "Search".
        -   Clicking on the correct search result to view the train's timeline.
        -   Parsing the timeline to extract details for every station on the route: scheduled arrival/departure, actual delay in minutes, and any reported reasons for disruptions.

## Tech Stack

-   **Python 3.x**
-   **Selenium**: For browser automation and scraping the main passenger portal.
-   **Requests-HTML**: For fetching and parsing the initial list of trains.
-   **WebDriverWait**: For robustly handling dynamic page elements and AJAX loading.

## Setup and Usage

### Prerequisites

-   Python 3.8+
-   Google Chrome browser installed.
-   **ChromeDriver** compatible with your version of Google Chrome. [Download here](https://googlechromelabs.github.io/chrome-for-testing/).
    > **Note:** Ensure that the `chromedriver` executable is either in your system's PATH or placed in the project's root directory.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd <repository-directory>
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    # For Windows
    python -m venv venv
    .\venv\Scripts\activate

    # For macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install the required dependencies from `requirements.txt`:**
    ```bash
    pip install -r requirements.txt
    ```

### Running the Scraper

To start the scraping process, simply run the main script:

```bash
python get_train_data.py
```

The script will start logging its progress to the console and to a file named `scraper_log_YYYY-MM-DD.log`. Upon completion, a JSON file will be created in the root directory.

## Output Structure

### JSON Output File

The script generates a file named `train_data_YYYY-MM-DD-hhmm.json`. It contains a list of train objects.

**Example `train_data.json`:**

```json
[
  {
    "domestic": "Krajowy",
    "number": "1026",
    "category": "IC",
    "name": "SKARYNA",
    "from": "Warszawa Wschodnia",
    "to": "Terespol",
    "occupancy": "Szacowana frekwencja poniżej 50%",
    "delay_info": [
      {
        "station_name": "Warszawa Wschodnia",
        "arrival_time": null,
        "departure_time": "07:27",
        "delay_minutes_arrival": null,
        "delay_minutes_departure": null,
        "distance_km_to_next": 36.3,
        "travel_time_to_next": "0h:19min",
        "difficulties_info": [
          "Inne przyczyny związane z utrzymaniem linii kolejowych",
          "Warszawa Wschodnia"
        ]
      },
      {
        "station_name": "Mińsk Mazowiecki",
        "arrival_time": "07:48",
        "departure_time": "07:49",
        "delay_minutes_arrival": 17,
        "delay_minutes_departure": 17,
        "distance_km_to_next": 54.4,
        "travel_time_to_next": "0h:28min",
        "difficulties_info": [
          "",
          ""
        ]
      }
    ],
    "date": "2025-06-23"
  }
]
```

### Log File

A log file named `scraper_log_YYYY-MM-DD.log` is created daily, capturing all events, warnings, and errors that occurred during the script's execution.

## Disclaimer

This tool is intended for educational and informational purposes only. The data is scraped from publicly accessible websites. Please use this script responsibly and be mindful of the websites' terms of service. The author is not responsible for any misuse of this tool.

## License

This project is licensed under the MIT License.