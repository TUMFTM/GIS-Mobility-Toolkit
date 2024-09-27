SET search_path TO $schema;
CREATE INDEX IF NOT EXISTS planet_osm_polygon_osm_id_idx ON $schema.planet_osm_polygon USING btree (osm_id);
CREATE INDEX IF NOT EXISTS planet_osm_polygon_way_osm_id_idx ON $schema.planet_osm_polygon USING gist (way, osm_id);

CREATE INDEX IF NOT EXISTS planet_osm_point_osm_id_idx ON $schema.planet_osm_point USING btree (osm_id);
CREATE INDEX IF NOT EXISTS planet_osm_point_way_osm_id_idx ON $schema.planet_osm_point USING gist (way, osm_id);