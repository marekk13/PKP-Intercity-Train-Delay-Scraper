CREATE INDEX IF NOT EXISTS idx_train_runs_date ON train_runs (date);
CREATE INDEX IF NOT EXISTS idx_train_runs_service_id ON train_runs (service_id);
CREATE INDEX IF NOT EXISTS idx_train_services_number ON train_services (number);
CREATE INDEX IF NOT EXISTS idx_run_stops_scheduled_arrival ON run_stops (scheduled_arrival);
CREATE INDEX IF NOT EXISTS idx_run_stops_station_id ON run_stops (station_id);



CREATE OR REPLACE VIEW view_train_summaries AS
SELECT 
    tr.id AS internal_id,
    to_char(tr.date, 'YYYYMMDD') || ts.number AS id,
    tr.date,
    ts.number,
    ts.name,
    tc.category_code AS category,
    s_start.name AS from_station,
    s_end.name AS to_station,
    ts.is_domestic,
    occ.status_description AS occupancy,
    
    (SELECT rs.scheduled_departure FROM run_stops rs WHERE rs.run_id = tr.id ORDER BY rs.stop_order ASC LIMIT 1) AS scheduled_departure,
    (SELECT rs.scheduled_arrival FROM run_stops rs WHERE rs.run_id = tr.id ORDER BY rs.stop_order DESC LIMIT 1) AS scheduled_arrival,
    (SELECT COALESCE(rs.delay_arrival_min, 0) FROM run_stops rs WHERE rs.run_id = tr.id ORDER BY rs.stop_order DESC LIMIT 1) AS delay_at_destination

FROM train_runs tr
JOIN train_services ts ON tr.service_id = ts.id
LEFT JOIN train_categories tc ON ts.category_id = tc.id
LEFT JOIN stations s_start ON ts.start_station_id = s_start.id
LEFT JOIN stations s_end ON ts.end_station_id = s_end.id
LEFT JOIN occupancies occ ON tr.occupancy_id = occ.id;


DROP FUNCTION IF EXISTS get_station_schedule(TEXT, DATE);

CREATE OR REPLACE FUNCTION get_station_schedule(p_station_name TEXT, p_date DATE DEFAULT NULL)

RETURNS TABLE (
    train_number TEXT,
    train_category TEXT,
    from_station TEXT,
    to_station TEXT,
    direction TEXT,
    scheduled_arrival TIME,
    scheduled_departure TIME,
    delay_arrival_min INT,
    delay_departure_min INT,
    is_delayed BOOLEAN,
    train_id TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ts.number::TEXT,
        tc.category_code::TEXT,
        s_start.name::TEXT AS from_station,
        s_end.name::TEXT AS to_station,
        s_end.name::TEXT AS direction,
        rs.scheduled_arrival,
        rs.scheduled_departure,
        rs.delay_arrival_min,
        rs.delay_departure_min,
        (COALESCE(rs.delay_arrival_min, 0) > 5 OR COALESCE(rs.delay_departure_min, 0) > 5) AS is_delayed,
        (to_char(tr.date, 'YYYYMMDD') || ts.number)::TEXT AS train_id
    FROM run_stops rs
    JOIN stations s_limit ON rs.station_id = s_limit.id
    JOIN train_runs tr ON rs.run_id = tr.id
    JOIN train_services ts ON tr.service_id = ts.id
    LEFT JOIN train_categories tc ON ts.category_id = tc.id
    LEFT JOIN stations s_start ON ts.start_station_id = s_start.id
    LEFT JOIN stations s_end ON ts.end_station_id = s_end.id
    WHERE 
        s_limit.name ILIKE ('%' || p_station_name || '%')
        AND (p_date IS NULL OR tr.date = p_date)
    ORDER BY 
        COALESCE(rs.scheduled_arrival, rs.scheduled_departure) ASC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

