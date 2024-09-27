SET search_path TO $schema;
CREATE MATERIALIZED VIEW IF NOT EXISTS $schema.planet_osm_buildings
AS SELECT pop.osm_id,
    rels.tags,
    pop.way_area
   FROM planet_osm_polygon pop
     LEFT JOIN ( SELECT planet_osm_rels.id,
            planet_osm_rels.way_off,
            planet_osm_rels.rel_off,
            planet_osm_rels.parts,
            planet_osm_rels.members,
            planet_osm_rels.tags
           FROM planet_osm_rels) rels ON pop.osm_id = rels.id
  WHERE pop.building IS NOT NULL OR ('building:part'::text <> ANY (rels.tags)) AND ('building'::text = ANY (rels.tags))
WITH DATA;

-- View indexes:
CREATE INDEX IF NOT EXISTS planet_osm_buildings_osm_id_idx ON $schema.planet_osm_buildings USING btree (osm_id);


