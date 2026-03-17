
-- kategorie pociągów (np. IC, TLK, EIP)
CREATE TABLE IF NOT EXISTS train_categories (
    id SERIAL PRIMARY KEY,
    category_code VARCHAR(10) UNIQUE NOT NULL
);
COMMENT ON TABLE train_categories IS 'Słownik kategorii pociągów (IC, TLK, EIP itp.).';

-- stacje kolejowe
CREATE TABLE IF NOT EXISTS stations (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);
COMMENT ON TABLE stations IS 'Unikalna lista wszystkich stacji kolejowych.';

-- poziomy frekwencji
CREATE TABLE IF NOT EXISTS occupancies (
    id SERIAL PRIMARY KEY,
    status_description TEXT UNIQUE NOT NULL
);
COMMENT ON TABLE occupancies IS 'Słownik opisów szacowanej frekwencji.';

-- typy utrudnień
CREATE TABLE IF NOT EXISTS difficulties (
    id SERIAL PRIMARY KEY,
    description TEXT UNIQUE NOT NULL
);
COMMENT ON TABLE difficulties IS 'Słownik unikalnych opisów utrudnień w ruchu pociągów.';

-- przejazdy pociągów
CREATE TABLE IF NOT EXISTS train_runs (
    id BIGSERIAL PRIMARY KEY,
    number VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    is_domestic BOOLEAN NOT NULL,
    date DATE NOT NULL,
    category_id INTEGER NOT NULL REFERENCES train_categories(id),
    start_station_id INTEGER NOT NULL REFERENCES stations(id),
    end_station_id INTEGER NOT NULL REFERENCES stations(id),
    occupancy_id INTEGER REFERENCES occupancies(id),

    CONSTRAINT uq_train_run UNIQUE (number, date)
);
COMMENT ON TABLE train_runs IS 'Główna tabela przechowująca informacje o każdym unikalnym przejeździe pociągu.';

-- przystanki na trasie
CREATE TABLE IF NOT EXISTS run_stops (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES train_runs(id) ON DELETE CASCADE,
    station_id INTEGER NOT NULL REFERENCES stations(id),
    stop_order INTEGER NOT NULL,
    scheduled_arrival TIME,
    scheduled_departure TIME,
    delay_arrival_min INTEGER,
    delay_departure_min INTEGER,
    distance_from_start_km NUMERIC(6, 1) -- np. 99999.9 km
);
COMMENT ON TABLE run_stops IS 'Szczegółowa trasa przejazdu z informacjami o każdym przystanku.';

-- Tabela N:M: Utrudnienia na Przystankach
CREATE TABLE IF NOT EXISTS run_stop_difficulties (
    stop_id BIGINT NOT NULL REFERENCES run_stops(id) ON DELETE CASCADE,
    difficulty_id INTEGER NOT NULL REFERENCES difficulties(id),
    location VARCHAR(255),

    -- Klucz główny złożony, aby ta sama para (przystanek, utrudnienie) nie mogła wystąpić wielokrotnie
    PRIMARY KEY (stop_id, difficulty_id)
);
COMMENT ON TABLE run_stop_difficulties IS 'Tabela łącząca przystanki z utrudnieniami (relacja wiele-do-wielu).';


CREATE INDEX IF NOT EXISTS idx_train_runs_category_id ON train_runs(category_id);
CREATE INDEX IF NOT EXISTS idx_train_runs_start_station_id ON train_runs(start_station_id);
CREATE INDEX IF NOT EXISTS idx_train_runs_end_station_id ON train_runs(end_station_id);

CREATE INDEX IF NOT EXISTS idx_run_stops_run_id ON run_stops(run_id);
CREATE INDEX IF NOT EXISTS idx_run_stops_station_id ON run_stops(station_id);

CREATE INDEX IF NOT EXISTS idx_run_stop_difficulties_stop_id ON run_stop_difficulties(stop_id);
CREATE INDEX IF NOT EXISTS idx_run_stop_difficulties_difficulty_id ON run_stop_difficulties(difficulty_id);