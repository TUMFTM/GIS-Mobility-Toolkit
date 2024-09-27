# -*- coding: utf-8 -*-
from __future__ import annotations

__license__ = "MIT"
__version__ = "0.1"
__status__ = "Production"
__author__ = "David Ziegler"

import os, yaml
import pandas as pd
import geopandas as gpd
from datetime import datetime
from itertools import chain
from typing import Annotated, List, Optional, Literal, Dict, Union, get_args
from enum import unique, Enum, IntEnum
from pydantic import BaseModel, Field, FilePath, DirectoryPath, computed_field
from ..common.config import TEST_ROOT
from ..framework.loaders import GeoFileOpsLoader, FileLoader
from ..framework.operators import SpatialOperatorAnnotated, SpatialOperator, SpatialTesselatorMeta, TesselationMethodsMeta


# Base DataLayers
class BaseLayerTypes(str, Enum):
    network = 'network'
    places = 'places'


class BaseDataLayer(BaseModel):
    name: str = None
    type: BaseLayerTypes = None
    mode: Literal['BaseDataLayer'] = 'BaseDataLayer'
    path_is_relative: bool = False

    _path: Union[None, FilePath] = None
    _path_base: Union[None, DirectoryPath] = None
    _loader: Union[None, GeoFileOpsLoader] = None
    _cache: Union[None, object] = None

    def __init__(self,
                 name: str,
                 type: BaseLayerTypes,
                 path: Union[None, str] = None,
                 data: Union[None, gpd.GeoDataFrame] = None,
                 **kwargs) -> None:
        super().__init__(name=name, type=type, **kwargs)
        self.__setattr__("_path", path)
        if data is not None:
            self.load()
            self._loader.set(data)

    def export(self, path: str):
        FileLoader(path).set(self._loader.content).save()
        return self

    def save(self):
        assert self._loader is not None, "No path defined on initializing for saving."
        self._loader.save()

    def load(self):
        path = self._path
        if self.path_is_relative:
            path = os.path.join(self._path_base, path)
        self.__setattr__("_loader", GeoFileOpsLoader(FileLoader(path)))

    def unpersist(self):
        self._cache = self._loader.content
        self._loader = None

    @property
    def content(self):
        if self._path is not None and self._loader is None:
            self.load()
        if self._loader is not None:
            return self._loader.content
        else:
            return self._cache

    @property
    def persistent(self):
        if self._loader is not None:
            return True
        else:
            return False

    @computed_field
    @property
    def path(self) -> Optional[FilePath]:
        if self._loader is not None:
            if self._path_base is None:
                return os.path.abspath(self._loader.file)
            else:
                return os.path.relpath(self._loader.file, self._path_base)
        return None

    def set_base_path(self, path: Union[None, DirectoryPath]):
        self.__setattr__("_path_base", path)
        if path is not None:
            self.path_is_relative = True
        else:
            self.path_is_relative = False
        return self

    class Config:
        use_enum_values = True


# Derived DataLayers
class DataLayer(BaseDataLayer):
    _origin: DataLayers
    _path: Union[None, FilePath] = None
    operator: SpatialOperatorAnnotated
    mode: Literal['DataLayer'] = 'DataLayer'

    def __init__(self,
                 name: str,
                 origin: DataLayers,
                 operator: SpatialOperatorAnnotated,
                 type: Union[None, BaseLayerTypes] = None,
                 path: Union[None, str] = None,
                 **kwargs) -> None:
        if type is None:
            type = origin.type
        # Quick hack for preventing faulty type error problem..
        if not isinstance(operator, dict):
            operator = operator.model_dump(exclude_none=True)
        super().__init__(name=name, type=type, operator=operator, path=path, **kwargs)
        self.__setattr__('_origin', origin)

    def apply_operation(self):
        if self._loader:
            self._loader.set(self.operator(self._origin))
        else:
            self._cache = self.operator.apply(self._origin)
        return self

    def make_persistent(self, path=None):
        self.__setattr__('_path', path)
        self.load()

    @property
    def content(self):
        if (self._loader is not None and self._loader.has_content() == False) or (self._cache is None):
            self.apply_operation()
        return super().content

    @computed_field
    @property
    def origin(self) -> str:
        return self._origin.name

    @property
    def origin_raw(self) -> DataLayers:
        return self._origin


# DataLayer Definition
DataLayers = Union[BaseDataLayer, DataLayer]
DataLayersAnnotate = Annotated[DataLayers, Field(discriminator="mode")]


# YamlConfigurationManager
class YamlConfigDefinition(BaseModel):
    author: Optional[str] = None
    last_update: Optional[datetime] = None
    references: Optional[str] = None
    version: int = 0
    layers: Dict[str, DataLayersAnnotate] = {}
    _base_path: Optional[str] = None

    def __init__(self, *args, base_path: Union[None, str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__setattr__("_base_path", base_path)
        for layer in self.layers.values():
            if isinstance(layer, DataLayer):
                layer._origin = self.layers.get(layer._origin, layer._origin)
                if hasattr(layer.operator, "join"):
                    layer.operator._join = self.layers.get(layer.operator.join, layer.operator.join)
            if layer.path_is_relative:
                layer.set_base_path(base_path)


# Configuration Manager
class PersistentManager:
    extension: str = ".ymlsmm"

    def __init__(self, path=None) -> None:
        self.root = None
        self.path = None
        if path is not None:
            self.load(path)
        else:
            self.config = YamlConfigDefinition()

    def add(self, item: DataLayers, name=None, use_relative_path=True):
        assert isinstance(
            item,
            get_args(DataLayers)), "Please set a valid data layer, either from the DataLayer, or BaseDataLayer type!"
        if isinstance(item, DataLayer):
            self.add(item.origin_raw)
        self.config.layers[name or item.name] = item
        if use_relative_path:
            item.set_base_path(self.root)
        return self

    def get(self, name: str):
        assert name in self.config.layers, "This data layer doesn't exist in the config!"
        return self.config.layers[name]

    def load(self, path):
        # Set root and path variables
        self.root = os.path.dirname(os.path.abspath(path))
        self.path = path
        # Read yaml configuration
        if os.path.isfile(path):
            with open(path, mode="r", encoding="utf-8") as yaml_file:
                self.config = YamlConfigDefinition(base_path=self.root, **yaml.safe_load(yaml_file))
        else:
            print("Configuration doesn't exist yet. Creating new one.")
            self.config = YamlConfigDefinition()
        return self

    def save(self, path=None):
        #Check path
        path = path or self.path
        assert path is not None, "Path to save config not set."
        #Save layers
        for _, layer in self.config.layers.items():
            if layer.persistent == True:
                layer.save()
        #Save config
        self.config.version += 1
        self.config.last_update = datetime.now()
        with open(path, mode="w", encoding="utf-8") as yaml_file:
            yaml.dump(self.config.model_dump(exclude_none=True), yaml_file)
        return self


if __name__ == "__main__":
    # Tests
    config_path = os.path.join(TEST_ROOT, "test_config.ymlsmm")
    geojson_path = os.path.join(TEST_ROOT, "format_tests", "geojson_test.geojson")
    data_layer = BaseDataLayer("base_layer", BaseLayerTypes.places, geojson_path)

    pm = PersistentManager(config_path)
    try:
        print(pm.get("transform_layer").content)
    except:
        print("Layer not found, presumably first initialization?")
    pm.add(data_layer)
    pm.add(
        DataLayer("transform_layer",
                  pm.get("base_layer"),
                  operator=SpatialTesselatorMeta(mask=TesselationMethodsMeta.h3, resolution=8)))
    pm.get("transform_layer").content
    pm.save()
