# -*- coding: utf-8 -*-
__author__ = "David Ziegler"
__copyright__ = "Copyright 2021, David Ziegler"
__credits__ = ["David Ziegler"]
__license__ = "MIT"
__version__ = "0.1"
__status__ = "Production"
__maintainer__ = "David Ziegler"
__email__ = "david.ziegler@tum.de"
__status__ = "Production"

import os
import pandas as pd
import geopandas as gpd
import sqlalchemy
import json
from ...common.config import FRAMEWORK_ROOT
from .parser import MapnikSqlParser
from ...common.sql import DBBase, SQLTemplateManager
from ...common.config import ConfigManager

CURRENT_ROOT = os.path.dirname(__file__)


class OSM_POI_SETUP:

    def __init__(self, config_path, external_sql_dir=None) -> None:
        self.config = ConfigManager(config_path).load().config
        if external_sql_dir is None:
            self.db = DBBase().setupDB(**self.config.psql_auth,
                                       extensions=["postgis", "pg_trgm", "btree_gin", "btree_gist", "parray_gin"])
        self.sql_template_manager = SQLTemplateManager(os.path.join(CURRENT_ROOT, "sql"))
        self.mapnik_parser = MapnikSqlParser(os.path.abspath(os.path.join(CURRENT_ROOT, "config", "mapnik.xml")))
        self.mapnik_parser.load_mapnik()
        self.config = self.config.osm_mid_mappings
        self.external_sql_dir = external_sql_dir
        self.iterator = 0

    def execute_sql(self, sql, placeholders={}):
        if self.external_sql_dir is None:
            self.db.executeSQL(sql, placeholders=placeholders)
        else:
            for key, value in placeholders.items():
                sql = sql.replace(f"${key}", value)
            with open(os.path.join(self.external_sql_dir, str(self.iterator) + "_setup.psql"), "w") as f:
                f.write(sql)
            self.iterator += 1

    def setup_osm_framework_base(self, schema="public"):
        if self.mapnik_parser.mapnik_loaded is not True:
            raise Exception("Please load mapnik first.")
        #Fn Helpers
        _sql = self.sql_template_manager.load("setup/0_fn_helpers")
        self.execute_sql(_sql, placeholders=dict(schema=schema))

        #Load POI Polygons
        _sql = self.sql_template_manager.load("setup/0_pre_setup_indexes")
        self.execute_sql(_sql, placeholders=dict(schema=schema))

        #Create POI extraction view based on Mapnik Configuration
        _pois = self.mapnik_parser.get_description("amenity-points")
        _sql = _pois["sql"]()
        _sql = f"""
        SET search_path TO $schema;
        CREATE MATERIALIZED VIEW IF NOT EXISTS $schema.planet_osm_mappings AS (
            {_sql}
        );
        CREATE INDEX IF NOT EXISTS planet_osm_mappings_osm_type_osm_id_idx ON $schema.planet_osm_mappings (osm_type,osm_id);
        CLUSTER $schema.planet_osm_mappings USING planet_osm_mappings_osm_type_osm_id_idx;
        """

        self.execute_sql(_sql, placeholders=dict(schema=schema))

        #Load Buildings
        _sql = self.sql_template_manager.load("setup/1_view_osm_buildings")
        self.execute_sql(_sql, placeholders=dict(schema=schema))

        #Load POI Polygons
        _sql = self.sql_template_manager.load("setup/1_view_osm_landuse")
        self.execute_sql(_sql, placeholders=dict(schema=schema))

        #Load POI Polygons
        _sql = self.sql_template_manager.load("setup/2_view_osm_poi_polygons")
        self.execute_sql(_sql, placeholders=dict(schema=schema))

    def update_framework_mappings(self, schema="public"):
        #Load MID Mappings
        _target_table = "planet_osm_poi_meta"
        _df = pd.read_excel(self.config.file, sheet_name=self.config.sheet, header=0)
        _df_columns = [
            "OSM_key", "OSM_tag", "MiD", "MiD_description", "SLP", "Relevant_for_charging", "Maximum_area",
            "Employees_per_sqm", "Employee_saturday_factor", "Employee_sunday_factor", "Visitor_per_sqm_per_day",
            "Visitor_saturday_factor", "Visitor_sunday_factor"
        ]

        #Load additional mappings
        def mapping_conversion(x, mappings={}):
            _map = mappings.get(x.OSM_key, None)
            if isinstance(_map, dict):
                if x.OSM_tag == "" or pd.isnull(x.OSM_tag):
                    return _map.get("!*", None)
                return _map.get(x.OSM_tag, _map.get("*", None))
            elif x.OSM_tag == "" or pd.isnull(x.OSM_tag):
                return _map
            else:
                return None

        for _map in self.config.additional_mappings:
            with open(_map) as f:
                for _name, _map in json.load(f).items():
                    _df[_name] = _df.apply(lambda x: mapping_conversion(x, mappings=_map), axis=1)
                    _df_columns.append(_name)

        _sql = self.sql_template_manager.load("functions/truncate_table")
        self.execute_sql(_sql, placeholders=dict(schema=schema, table=_target_table))

        _df[_df_columns].to_sql(_target_table,
                                sqlalchemy.create_engine(self.db.con_url),
                                schema=schema,
                                if_exists="append")
        _sql = f"""
            SET search_path TO $schema;
            ALTER TABLE $table ADD COLUMN IF NOT EXISTS full_category text;
            UPDATE $table m SET full_category = array_to_string(ARRAY[m."OSM_key" , NULLIF(m."OSM_tag", '')], '_');
            CREATE INDEX IF NOT EXISTS $table_osm_index_idx ON $table USING btree (index);
            CREATE INDEX IF NOT EXISTS $table_full_category_idx ON $table USING btree (full_category);
            CREATE INDEX IF NOT EXISTS $table_osm_key_full_category_idx ON $table USING btree("OSM_key", full_category);
            CREATE INDEX IF NOT EXISTS $table_osm_key_tag_full_category_idx ON $table USING btree ("OSM_key", "OSM_tag", full_category);
            ALTER TABLE $table ADD COLUMN IF NOT EXISTS "jsonb" jsonb;
            UPDATE $table SET "jsonb" = ('{{"' || "OSM_key" || '":{{"' || COALESCE("OSM_tag", '') || '":true}}}}')::jsonb
        """
        self.execute_sql(_sql, placeholders=dict(schema=schema, table=_target_table))

    def query_landuse(self, boundary, schema="public"):
        boundary = self.sql_template_manager.load("boundary/" + boundary)
        sql = self.sql_template_manager.load("query/osm_landuse_extract",
                                             replacements={
                                                 "boundary": boundary,
                                                 "category": "landuse_munich_mappings",
                                                 "schema": schema
                                             })
        gdf = gpd.GeoDataFrame.from_postgis(sql, con=self.db.conn, geom_col='geom')
        return gdf


if __name__ == '__main__':
    config = os.path.join(FRAMEWORK_ROOT, 'core', 'osm', 'config', "setup.yml")
    sql_path = os.path.join(FRAMEWORK_ROOT, "tmp", "sql")
    os.makedirs(sql_path, exist_ok=True)
    ops = OSM_POI_SETUP(config)
    ops.update_framework_mappings(schema="osm")
