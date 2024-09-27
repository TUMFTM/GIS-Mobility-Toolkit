SET search_path TO $schema;
CREATE MATERIALIZED VIEW IF NOT EXISTS osm.planet_osm_poi_polygons
AS WITH pow AS (
          SELECT pow.osm_id, pow.way, pow.way_area
          FROM planet_osm_polygon pow, planet_osm_buildings pob 
          WHERE pob.osm_id = pow.osm_id
        ), mp AS (
          SELECT mp.way, mp.feature, mp.shop 
          FROM planet_osm_mappings mp
          WHERE mp.osm_type = 'point'
        ),
        selection_pnt AS (
          SELECT pow.osm_id AS osm_id, pow.way AS way, pow.way_area AS area,
          CASE
              WHEN mp.feature <> 'shop'::text THEN mp.feature
              ELSE (mp.feature || '_'::text) || mp.shop
          END AS feature
          FROM mp, pow 
          WHERE pow.way && mp.way AND ST_INTERSECTS(pow.way, mp.way) 
            ), selection_way AS (
            SELECT pop.osm_id,
                pop.way,
                pop.way_area AS area,
                    CASE
                        WHEN mp.feature <> 'shop'::text 
                        THEN mp.feature
                        ELSE (mp.feature || '_'::text) || mp.shop
                    END AS feature
              FROM planet_osm_polygon pop
              JOIN planet_osm_mappings mp ON mp.osm_type = 'polygon'::text AND mp.osm_id = pop.osm_id
        )
 SELECT _.osm_id AS way_id,
    min(_.area) AS area,
    array_agg(_.feature) AS features,
    jsonb_merge_deep(array_agg(_.feature_json_cnt)) AS features_cnt_json,
    st_centroid(min(_.way)::geometry) AS center
   FROM ( SELECT __1.osm_id,
            min(__1.area) AS area,
            min(__1.way::text) AS way,
            __1.feature,
            FORMAT($${%s:{%s:%s}}$$,
              to_jsonb(split_part(__1.feature, '_'::text, 1)::text), 
              to_jsonb(
                CASE
                  WHEN POSITION('_' IN __1.feature) = 0 THEN ''::text
                  ELSE SUBSTRING(__1.feature FROM POSITION('_' IN __1.feature) + 1)::text
                END
              ), 
              to_jsonb(count(__1.feature))::NUMERIC 
            )::jsonb AS feature_json_cnt
           FROM ( SELECT sp.osm_id,
                    sp.way,
                    sp.area,
                    sp.feature
                   FROM selection_pnt sp
                UNION ALL
                 SELECT sw.osm_id,
                    sw.way,
                    sw.area,
                    sw.feature
                   FROM selection_way sw
                  WHERE NOT (EXISTS ( SELECT 1
                           FROM selection_pnt
                          WHERE st_intersects(sw.way, selection_pnt.way) AND sw.feature = selection_pnt.feature
                         LIMIT 1))) __1
          GROUP BY __1.osm_id, __1.feature
          ORDER BY __1.osm_id, __1.feature) _
  GROUP BY _.osm_id
  ORDER BY _.osm_id
WITH DATA;

-- View indexes:
CREATE INDEX planet_osm_poi_polygons_way_id_idx ON $schema.planet_osm_poi_polygons USING btree (way_id);
CREATE INDEX planet_osm_poi_polygons_center_way_id_idx ON $schema.planet_osm_poi_polygons USING gist (center, way_id);
CREATE INDEX planet_osm_poi_polygons_features_json_idx ON $schema.planet_osm_poi_polygons USING gin (features_json jsonb_path_ops);
CREATE INDEX planet_osm_poi_polygons_features_json_idx1 ON $schema.planet_osm_poi_polygons USING gin (features_json);
CLUSTER $schema.planet_osm_poi_polygons USING planet_osm_poi_polygons_center_way_id_idx;
