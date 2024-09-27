from __future__ import annotations
import os, fiona
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio
import rioxarray as rxr
import geofileops as gfo
import inspect
from shapely.geometry import shape, Point
from typing import List, Optional, Literal, Dict, Union
from pydantic import BaseModel

from ..common.config import TEST_ROOT


#Base loader class
class GeoPandasBase(object):
    extension: str = None
    enable: bool = True

    def __init__(self, path: Union[str, any]) -> None:
        self._content = None
        assert (self.extension is not None)
        if isinstance(path, tuple(loader_classes.values())):
            self._content = path
            path, _ = os.path.splitext(path.file)
            tail = None
        else:
            _, tail = os.path.splitext(path)
        if not tail:
            self.file = path + self.extension
        else:
            self.file = path

    @property
    def content(self):
        self.load()
        if self._content is None and not os.path.isfile(self.file):
            self._content = gpd.GeoDataFrame()
        return self._content

    @content.setter
    def content(self, gdf: Union[gpd.GeoDataFrame, pd.DataFrame]):
        self.set(gdf)

    def set(self, gdf: Union[gpd.GeoDataFrame, pd.DataFrame]):
        self._content = gdf
        return self

    def get(self):
        return self._content

    def load(self):
        if isinstance(self._content, tuple(loader_classes.values())):
            self._content = self._content.content
        return self

    def save(self):
        raise Exception("Not implemented yet")

    def has_content(self):
        return self._content is not None


#Geofileops Loader
class GeoFileOpsLoader(GeoPandasBase):
    extension: str = ".gpkg"

    def __init__(self, path) -> None:
        super().__init__(path)

    def load(self):
        super().load()
        if self._content is None:
            if os.path.isfile(self.file):
                self._content = gfo.read_file(self.file)
        return self

    def save(self):
        gfo.to_file(self.content, self.file)
        return self


# Geoparquet Loader
class GeoparquetLoader(GeoPandasBase):
    extension: str = ".gpq"

    def __init__(self, path) -> None:
        super().__init__(path)

    def load(self):
        super().load()
        if self._content is None:
            if os.path.isfile(self.file):
                self._content = gpd.read_parquet(self.file)
        return self

    def save(self):
        self._content.to_parquet(self.file)
        return self


# GeoJSON Loader
class GeoJSONLoader(GeoPandasBase):
    extension: str = ".geojson"

    def __init__(self, path) -> None:
        super().__init__(path)

    def load(self):
        super().load()
        if self._content is None:
            if os.path.isfile(self.file):
                self._content = gpd.read_file(self.file)
        return self

    def save(self):
        self._content.to_parquet(self.file)
        return self


# Shapefile Loader
class ShapeFileLoader(GeoPandasBase):
    extension: str = ".shp"

    def __init__(self, path) -> None:
        super().__init__(path)

    def load(self):
        super().load()
        if self._content is None:
            if os.path.isfile(self.file):
                self._content = gpd.read_file(self.file)
        return self

    def save(self):
        self._content.to_parquet(self.file)
        return self


# KML Loader
class KmlLoader(GeoPandasBase):
    extension: str = ".kml"

    def __init__(self, path) -> None:
        super().__init__(path)

    def load(self):
        super().load()
        if self._content is None:
            if os.path.isfile(self.file):
                fiona.supported_drivers['KML'] = 'rw'
                self._content = gpd.read_file(self.file, driver='KML')
        return self

    def save(self):
        self._content.to_parquet(self.file)
        return self


# GML Loader
class GmlLoader(GeoPandasBase):
    extension: str = ".gml"

    def __init__(self, path) -> None:
        super().__init__(path)

    def load(self):
        super().load()
        if self._content is None:
            if os.path.isfile(self.file):
                self._content = gpd.read_file(self.file)
        return self

    def save(self):
        self._content.to_parquet(self.file)
        return self


# GPKG Loader alternative
class GpkgLoader(GeoPandasBase):
    extension: str = ".gpkg"
    enable: bool = False

    def __init__(self, path) -> None:
        super().__init__(path)

    def load(self):
        super().load()
        if self._content is None:
            if os.path.isfile(self.file):
                self._content = gpd.read_file(self.file)
        return self

    def save(self):
        self._content.to_parquet(self.file)
        return self


# TIFF
class TiffLoader(GeoPandasBase):
    extension: str = ".tif"

    def __init__(self, path) -> None:
        super().__init__(path)

    def load(self):
        super().load()
        if self._content is None:
            if os.path.isfile(self.file):
                dataarray = rxr.open_rasterio(self.file)
                x, y, values = dataarray.x.values, dataarray.y.values, dataarray.values
                x, y = np.meshgrid(x, y)
                x, y, values = x.flatten(), y.flatten(), values.flatten()
                self._content = gpd.GeoDataFrame.from_dict({
                    'geometry': [Point(x, y) for x, y in zip(y, x)],
                    'value': values
                })
        return self

    def save(self):
        self._content.to_parquet(self.file)
        return self


class GeoTiffLoader(TiffLoader):
    extension: str = ".geotiff"


loader_classes = {
    cls.extension: cls for name, cls in globals().items() if inspect.isclass(cls) and issubclass(cls, GeoPandasBase)
}


def FileLoader(path: str):
    _, tail = os.path.splitext(path)
    assert tail, "Need an extension inside the file path."
    assert tail in loader_classes, "Unknown file extension, no automatic loading supported."
    return loader_classes[tail](path)


if __name__ == "__main__":
    # Test routine
    loader = GpkgLoader(FileLoader(os.path.join(TEST_ROOT, "format_tests\\tiff.tif")))
    gdf = loader.content
    print(gdf)
