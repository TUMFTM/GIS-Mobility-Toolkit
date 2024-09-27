SET search_path TO $schema;
CREATE MATERIALIZED VIEW IF NOT EXISTS $schema.planet_osm_landuse
AS WITH selection_way AS (
         SELECT pop.osm_id,
            mp.feature,
            pop.way AS geom
           FROM planet_osm_polygon pop
             JOIN planet_osm_mappings mp ON mp.osm_type = 'polygon'::text AND mp.osm_id = pop.osm_id
          WHERE mp.feature <> 'building_yes'::text
        )
 SELECT selection_way.osm_id,
    selection_way.feature,
    selection_way.geom
   FROM selection_way
WITH DATA;

-- View indexes:
CREATE INDEX IF NOT EXISTS planet_osm_landuse_geom_idx ON $schema.planet_osm_landuse USING gist (geom);
CREATE INDEX IF NOT EXISTS planet_osm_landuse_osm_id_geom_idx ON $schema.planet_osm_landuse USING gist (osm_id, geom);