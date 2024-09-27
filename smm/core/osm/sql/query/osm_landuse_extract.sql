SET search_path TO $schema, public;
SELECT osm_id, geom, full_categories, osm_feature, visitor_capacity, area
	FROM (
        WITH
        _boundary AS ($boundary),
        boundary AS (
            SELECT
              ST_TRANSFORM (_boundary.shape, 3857) shape
            FROM
              _boundary
        ),
        landuse AS (
          SELECT pl.geom as shape, pocm.$category full_category
          FROM
          boundary b,
          planet_osm_landuse pl
          INNER JOIN planet_osm_poi_meta pocm ON (pocm."OSM_tag" ISNULL AND pocm."OSM_key" = pl.feature) 
                            OR (pocm.full_category = pl.feature)
          WHERE pl.geom && b.shape
        ),
        landuse_buildings AS (
          SELECT DISTINCT ON (popp.way_id)  popp.*, l.full_category landuse_category
          FROM landuse l, planet_osm_poi_polygons popp
          WHERE ST_INTERSECTS(l.shape, popp.center)
          ORDER BY popp.way_id
        ),
        area_buildings AS (
          SELECT popp.*
          FROM boundary b, planet_osm_poi_polygons popp
          WHERE ST_INTERSECTS(b.shape, popp.center)
        ),
        area_landuse_buildings AS (
          SELECT ab.*, lb.landuse_category
          FROM area_buildings ab
          LEFT JOIN landuse_buildings lb ON lb.way_id = ab.way_id
        ),
        area_buildings_geom AS (
          SELECT ab.*, pop.way geom
          FROM boundary b, area_landuse_buildings ab
          INNER JOIN planet_osm_polygon pop ON ab.way_id = pop.osm_id
          WHERE pop.way && b.shape
          ORDER BY pop.osm_id ASC
        ),
        buildings AS (
          SELECT ab.*, UNNEST(ab.features) feature
          FROM area_buildings_geom ab
        ),
        buildings_mapped AS (
          SELECT *, 
          (
            ROW_NUMBER() OVER (
            PARTITION BY osm_id
            ORDER BY "max_area" DESC
            )
          ) poi_no
          FROM ( 
            SELECT
            pwc.way_id osm_id,
            pwc.area "area",
            CASE
              WHEN ARRAY_LENGTH(array_distinct (pwc.features), 1) = 1 THEN pwc.area
              ELSE least(pwc.area, COALESCE(pocm."Maximum_area", pwc.area))
            END "max_area",
            COALESCE(pocm."Employees_per_sqm", 0) employees_sqm,
            COALESCE(pocm."Visitor_per_sqm_per_day", 0) visitors_sqm,
            CASE
              WHEN pocm."OSM_key" = 'building'
              AND pocm."OSM_tag" = 'yes' THEN COALESCE(pwc.landuse_category, pocm.$category)
              ELSE pocm.$category
            END full_categories,
            feature osm_feature,
            pocm."OSM_key",
            pocm."OSM_tag",
            pwc.center,
            pwc.geom
            FROM buildings pwc
            INNER JOIN planet_osm_poi_meta pocm 
              ON (pocm."OSM_tag" ISNULL AND pocm."OSM_key" = pwc.feature)
              OR (pocm.full_category = pwc.feature)
            WHERE (
                pocm."OSM_key" <> 'landuse'
                OR pocm."OSM_tag" = 'industrial'
                OR pocm."OSM_tag" = 'education'
                OR pocm."OSM_tag" = 'retail'
                OR pocm."OSM_tag" = 'commercial'
                OR pocm."OSM_tag" = 'residential'
	            OR pocm."OSM_tag" = 'allotments'
	            OR pocm."OSM_tag" = 'cemetery'
	            OR pocm."OSM_tag" = 'grass'
	            OR pocm."OSM_tag" = 'meadow'
	            OR pocm."OSM_tag" = 'forest'
              ) AND pocm."OSM_key" <> ''
            ORDER BY pwc.way_id ASC
          ) pwc
        ),
        buildings_area_agg AS (
          SELECT
          osm_id,
          "OSM_key",
          "OSM_tag",
          "area" "area_full",
          area_aggregator("area"::float, "max_area"::float, "poi_no"::int) OVER (
            PARTITION BY pwc."osm_id" ORDER BY "poi_no" DESC
          ) "area_cum",
          osm_feature,
          full_categories full_categories,
          employees_sqm,
          visitors_sqm,
          poi_no,
          geom
          FROM buildings_mapped pwc
        )
    SELECT
    osm_id,
    area_full,
    COALESCE(
      "area_cum" - LAG("area_cum") OVER (
      PARTITION BY "osm_id"
      ORDER BY
        "poi_no" DESC
      ),
      "area_cum"
    ) "area",
    COALESCE(
      "area_cum" - LAG("area_cum") OVER (
      PARTITION BY "osm_id"
      ORDER BY
        "poi_no" DESC
      ),
      "area_cum"
    ) * employees_sqm + COALESCE(
      "area_cum" - LAG("area_cum") OVER (
      PARTITION BY "osm_id"
      ORDER BY
        "poi_no" DESC
      ),
      "area_cum"
    ) * visitors_sqm "visitor_capacity",
    full_categories,
    osm_feature,
    geom
	FROM buildings_area_agg sw
) _;