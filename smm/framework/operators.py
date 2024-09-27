# -*- coding: utf-8 -*-
from __future__ import annotations

__license__ = "MIT"
__version__ = "0.1"
__status__ = "Production"

import os
import pandas as pd
import geopandas as gpd
import geofileops as gfo

from enum import unique, Enum, IntEnum
from typing import Annotated, List, Optional, Literal, Dict, Union
from pydantic import BaseModel, Field, FilePath, DirectoryPath, computed_field
from srai.joiners import IntersectionJoiner
from srai.regionalizers import H3Regionalizer, S2Regionalizer
from ..common.config import TMP_ROOT

DataLayers = Union["BaseDataLayer", "DataLayer"]


# Discretization to free shapes
class SpatialDiscretizer(BaseModel):
    type: Literal['discretize'] = "discretize"
    _tmp_dir: str

    def __init__(self, tmp_dir: str = TMP_ROOT) -> None:
        self._tmp_dir = tmp_dir

    def area_intersection(self,
                          base_df: Union[str, gpd.GeoDataFrame],
                          mask_df: Union[str, gpd.GeoDataFrame],
                          crs,
                          hull_clip=True):
        #TODO: Add direct path support
        # Define paths
        target_data_gpkg = os.path.join(self._tmp_dir, "input1.gpkg")
        mask_data_gpkg = os.path.join(self._tmp_dir, "input2.gpkg")
        mask_data_hull_gpkg = os.path.join(self._tmp_dir, "input2_hull.gpkg")
        output_path = os.path.join(self._tmp_dir, "output.gpkg")

        # Setup data
        df1 = base_df.copy().to_crs(crs)
        df2 = mask_df.copy().to_crs(crs)
        dfs = {}
        if hull_clip:
            #TODO: Make faster via geofileops dissolve
            df2_hull = gpd.GeoDataFrame(geometry=gpd.GeoSeries(df2.unary_union.convex_hull), crs=df2.crs)
            dfs[mask_data_hull_gpkg] = df2_hull

        dfs = {**dfs, **{target_data_gpkg: df1, mask_data_gpkg: df2}}
        for _path, _df in dfs.items():
            _df["geom_area"] = _df.geometry.area
            if "geometry" not in _df:
                _df.rename_geometry("geometry", inplace=True)
            gfo.to_file(_df, _path)

        # Calculate join
        gfo.join_by_location(mask_data_gpkg,
                             target_data_gpkg,
                             output_path=output_path,
                             area_inters_column_name="intersect_area",
                             input1_columns=["fid"] + list(filter(lambda x: x != "geometry", df2.columns)),
                             input2_columns=["fid"] + list(filter(lambda x: x != "geometry", df1.columns)),
                             force=True)
        joined = gfo.read_file(output_path)

        # Calculate hull and join
        joined["l1_l2_scale"] = joined["intersect_area"] / joined.groupby(by="l2_fid").transform(
            "sum", "intersect_area")["intersect_area"]
        if hull_clip:
            gfo.join_by_location(target_data_gpkg,
                                 mask_data_hull_gpkg,
                                 output_path=output_path,
                                 area_inters_column_name="intersect_area",
                                 input1_columns=["fid"],
                                 input2_columns=["fid"],
                                 force=True)
            joined_hull = gfo.read_file(output_path)
            joined = pd.merge(joined,
                              joined_hull[["l1_fid",
                                           "intersect_area"]].rename(columns={"intersect_area": "l2_hull_area"}),
                              left_on="l2_fid",
                              right_on="l1_fid",
                              how="left",
                              suffixes=(None, "_y")).drop(columns='l1_fid_y')
            joined["l1_l2_scale"] *= joined["l2_hull_area"] / joined["l2_geom_area"]
        return joined

    def discretize(self, input: Union[str, gpd.GeoDataFrame], mask: Union[str, gpd.GeoDataFrame], crs, hull_clip=True):
        intersection = self.area_intersection(input, mask, crs=crs, hull_clip=hull_clip)
        columns = [c for c in intersection.columns if c.startswith8("l2_")]
        intersection[columns] *= intersection["l1_l2_scale"]
        return intersection[columns + ["l1_l2_scale"]].rename({c: c.substring(3, None) for c in columns})


class SpatialDiscretizerMeta(SpatialDiscretizer):
    mask: DataLayers
    hull_clip: bool = True

    def apply(self, input: DataLayers):
        return self.discretize(input.content, self.mask, input.content.crs, hull_clip=self.hull_clip)

    class Config:
        use_enum_values = True


# Tesselation logic
@unique
class TesselationMethodsMeta(str, Enum):
    s2 = 's2'
    h3 = 'h3'

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class SpatialTesselator(BaseModel):
    type: Literal['tesselate'] = "tesselate"

    def tesselate(self, data: DataLayers, mask, resolution):
        assert TesselationMethodsMeta.has_value(mask), "Tesselation Method doesn't exist."
        if mask == TesselationMethodsMeta.h3:
            regionalizer = H3Regionalizer(resolution=resolution)
        elif mask == TesselationMethodsMeta.s2:
            regionalizer = S2Regionalizer(resolution=resolution)
        regions = regionalizer.transform(data.content)
        joint_gdf = IntersectionJoiner().transform(regions, data.content, return_geom=True).reset_index()
        return joint_gdf


class SpatialTesselatorMeta(SpatialTesselator):
    mask: TesselationMethodsMeta
    resolution: int

    def apply(self, input: DataLayers):
        return SpatialTesselator.tesselate(self, input, self.mask, self.resolution)

    class Config:
        use_enum_values = True


# Discretization to free shapes
class SpatialJoin(BaseModel):
    type: Literal['join'] = "join"
    _tmp_dir: str

    def __init__(self, *args, tmp_dir: str = TMP_ROOT, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__setattr__('_tmp_dir', tmp_dir)

    def layer_join(self, base_df: Union[str, gpd.GeoDataFrame], join_df: Union[str, gpd.GeoDataFrame], crs):
        #TODO: Add direct path support
        # Define paths
        target_data_gpkg = os.path.join(self._tmp_dir, "input1.gpkg")
        join_data_gpkg = os.path.join(self._tmp_dir, "input2.gpkg")
        output_path = os.path.join(self._tmp_dir, "output.gpkg")

        # Setup data
        df1 = base_df.copy().to_crs(crs)
        df2 = join_df.copy().to_crs(crs)
        dfs = {}

        dfs = {**dfs, **{target_data_gpkg: df1, join_data_gpkg: df2}}
        for _path, _df in dfs.items():
            _df["geom_area"] = _df.geometry.area
            if "geometry" not in _df:
                _df.rename_geometry("geometry", inplace=True)
            gfo.to_file(_df, _path)

        # Calculate join
        gfo.join_by_location(join_data_gpkg,
                             target_data_gpkg,
                             output_path=output_path,
                             area_inters_column_name="intersect_area",
                             input1_columns=["fid"] + list(filter(lambda x: x != "geometry", df2.columns)),
                             input2_columns=["fid"] + list(filter(lambda x: x != "geometry", df1.columns)),
                             force=True)
        joined = gfo.read_file(output_path)
        return joined


class SpatialJoinMeta(SpatialJoin):
    _join: Union[None, DataLayers] = None

    def __init__(self, join: DataLayers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__setattr__('_join', join)

    def apply(self, base: DataLayers):
        return self.layer_join(base.content, self._join.content, crs=base.content.crs)

    @computed_field
    @property
    def join(self) -> str:
        return self._join if isinstance(self._join, str) else self._join.name

    class Config:
        use_enum_values = True


SpatialOperator = Union[SpatialDiscretizerMeta, SpatialTesselatorMeta, SpatialJoinMeta]
SpatialOperatorAnnotated = Annotated[SpatialOperator, Field(discriminator="type")]

if __name__ == "__main__":
    from ..common.config import TEST_ROOT
    from .persistent import PersistentManager, DataLayer
    pm = PersistentManager().load(os.path.join(TEST_ROOT, "test_config.ymlsmm"))
    tesselation_layer = DataLayer("transform_layer",
                                  pm.get("base_layer"),
                                  operator=SpatialTesselatorMeta(mask=TesselationMethodsMeta.h3, resolution=8))
    print(tesselation_layer.content.head())
    print(pm.get("transform_layer").content.head())
