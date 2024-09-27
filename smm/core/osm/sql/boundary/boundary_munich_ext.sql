SELECT
    ST_BUFFER (ST_SetSRID (ST_UNION (geom), 4326), 0.3) AS shape
FROM
    nuts.nuts_2021_1m
WHERE
    nuts_id IN ('DE21H', 'DE212')