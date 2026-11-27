-- Part 3, Serve: the three questions, answered from PostgreSQL.
-- Run with:  docker compose exec postgres psql -U divvy -f /serve/queries.sql
\pset footer off

\echo
\echo 'Q1  Which ten stations start the most trips, and how busy are they for their number of docks?'
SELECT rank, station_name, dpcapacity AS docks, trips_started, trips_per_dock_per_day
FROM q1_top_stations
ORDER BY rank;

\echo
\echo 'Q2  Do annual members and day-pass riders ride at different times of day, and for how long?'
\echo '    Share of each rider type''s trips starting in each hour, and median minutes.'
SELECT hour,
       max(pct_of_type)    FILTER (WHERE usertype = 'Subscriber') AS member_pct,
       max(pct_of_type)    FILTER (WHERE usertype = 'Customer')   AS daypass_pct,
       max(median_minutes) FILTER (WHERE usertype = 'Subscriber') AS member_median_min,
       max(median_minutes) FILTER (WHERE usertype = 'Customer')   AS daypass_median_min
FROM q2_usertype_by_hour
GROUP BY hour
ORDER BY hour;

-- The stream replays 2017 trips in start_time order, so "this hour" is the
-- hour of the trip that arrived last, not the clock on the wall.
\echo
\echo 'Q3  How many trips are starting at each station this hour?  (hour of the latest trip received)'
SELECT c.hour_start AS this_hour, c.station_id, s.station_name, c.trips,
       s.dpcapacity AS docks
FROM station_hourly_counts c
LEFT JOIN stations s USING (station_id)
WHERE c.hour_start = (SELECT date_trunc('hour', start_time) FROM trip_starts
                      ORDER BY ingest_seq DESC LIMIT 1)
ORDER BY c.trips DESC, c.station_id;
