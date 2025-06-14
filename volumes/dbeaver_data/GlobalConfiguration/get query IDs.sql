SELECT initial_query_id, query, event_time
FROM system.query_log
WHERE event_date >= (today() - 3) AND type = 2
LIMIT 10;