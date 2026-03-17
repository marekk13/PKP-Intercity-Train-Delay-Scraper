CREATE INDEX IF NOT EXISTS idx_train_runs_date ON train_runs (date);
CREATE INDEX IF NOT EXISTS idx_train_runs_number ON train_runs (number);
CREATE INDEX IF NOT EXISTS idx_run_stops_scheduled_arrival ON run_stops (scheduled_arrival);
CREATE INDEX IF NOT EXISTS idx_run_stops_station_id ON run_stops (station_id);


CREATE OR REPLACE VIEW view_train_summaries AS
SELECT 
    tr.id AS internal_id,
    to_char(tr.date, 'YYYYMMDD') || tr.number AS id,
    tr.date,
    tr.number,
    tr.name,
    tc.category_code AS category,
    s_start.name AS from_station,
    s_end.name AS to_station,
    tr.is_domestic,
    occ.status_description AS occupancy,
    
    (SELECT rs.scheduled_departure FROM run_stops rs WHERE rs.run_id = tr.id ORDER BY rs.stop_order ASC LIMIT 1) AS scheduled_departure,
    (SELECT rs.scheduled_arrival FROM run_stops rs WHERE rs.run_id = tr.id ORDER BY rs.stop_order DESC LIMIT 1) AS scheduled_arrival,
    (SELECT COALESCE(rs.delay_arrival_min, 0) FROM run_stops rs WHERE rs.run_id = tr.id ORDER BY rs.stop_order DESC LIMIT 1) AS delay_at_destination

FROM train_runs tr
LEFT JOIN train_categories tc ON tr.category_id = tc.id
LEFT JOIN stations s_start ON tr.start_station_id = s_start.id
LEFT JOIN stations s_end ON tr.end_station_id = s_end.id
LEFT JOIN occupancies occ ON tr.occupancy_id = occ.id;



CREATE OR REPLACE FUNCTION get_station_schedule(p_station_name TEXT, p_date DATE DEFAULT NULL)
RETURNS TABLE (
    train_number TEXT,
    train_category TEXT,
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
        tr.number::TEXT,
        tc.category_code::TEXT,
        s_end.name::TEXT AS direction,
        rs.scheduled_arrival,
        rs.scheduled_departure,
        rs.delay_arrival_min,
        rs.delay_departure_min
        (rs.delay_arrival_min > 5 OR rs.delay_departure_min > 5) AS is_delayed,
        (to_char(tr.date, 'YYYYMMDD') || tr.number)::TEXT AS train_id
    FROM run_stops rs
    JOIN stations s_limit ON rs.station_id = s_limit.id
    JOIN train_runs tr ON rs.run_id = tr.id
    LEFT JOIN train_categories tc ON tr.category_id = tc.id
    LEFT JOIN stations s_end ON tr.end_station_id = s_end.id
    WHERE 
        s_limit.name ILIKE ('%' || p_station_name || '%')
        AND (p_date IS NULL OR tr.date = p_date)
    ORDER BY 
        COALESCE(rs.scheduled_arrival, rs.scheduled_departure) ASC;
END;
$$ LANGUAGE plpgsql;
