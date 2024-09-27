SELECT ST_SetSRID(ST_UNION(geom),4326) as shape FROM nuts.nuts_2021_1m WHERE nuts_id IN ('DE')
