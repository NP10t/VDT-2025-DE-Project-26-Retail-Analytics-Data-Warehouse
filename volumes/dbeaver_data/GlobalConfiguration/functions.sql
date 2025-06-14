WITH
    [
        '147e8b72-9d3c-4b68-8fd8-ca0763fc0b41',
        '0ea73238-50cb-4203-a695-c42d80a14931',
        '7bb7ea54-69e9-43e1-9daa-968697fbe29f'
    ] AS query_ids
SELECT
    PE.1 AS metric,
    qid AS query_id,
    sum(PE.2) AS value
FROM clusterAllReplicas(default, system.query_log)
ARRAY JOIN ProfileEvents AS PE
INNER JOIN (
    SELECT arrayJoin(query_ids) AS qid
) AS queries
ON initial_query_id = qid
WHERE event_date >= today() - 3
  AND type = 2
GROUP BY
    metric, query_id
ORDER BY
    metric, query_id;

CREATE FUNCTION get_latest_query
AS (pattern) -> (
    SELECT query_id
    FROM clusterAllReplicas(default, system.query_log)
    WHERE event_date >= (today() - 3)
        AND type = 2
        AND query LIKE pattern
    ORDER BY event_time DESC
    LIMIT 1
);

DROP function get_latest_query;

SELECT get_latest_query('-- con%');

WITH
    initial_query_id = ( SELECT get_latest_query('-- con%') ) as first,
    initial_query_id = ( SELECT get_latest_query('-- bitmap%') ) as second
  SELECT metric, v1 as replacing_array_year_month, v2 as replacing_array_YYMM, dB
  from
 ( SELECT
      PE.1 AS metric,
      sumIf(PE.2, first) AS v1,
      sumIf(PE.2, second) AS v2,
      10 * log10(v2 / v1) AS dB,
      round(((v2 - v1) / if(v2 > v1, v2, v1)) * 100, 2) AS perc,
      bar(abs(perc), 0, 100, 33) AS bar
  FROM clusterAllReplicas(default, system.query_log)
  ARRAY JOIN ProfileEvents AS PE
  WHERE (first OR second) AND (event_date >= (today() - 3)) AND (type = 2)
  GROUP BY metric
  HAVING (v1 != v2) AND (abs(perc) >= 0)
  ORDER BY
      dB DESC,
      v2 DESC,
      metric ASC
);