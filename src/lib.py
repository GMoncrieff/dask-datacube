"""
All the geospatial and data processing stuff goes here.
"""

import logging
import os
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter, time
from typing import Generator

import ee
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from cartopy.feature import LAND
from odc.geo.geobox import GeoBox, GeoboxTiles
from odc.geo.xr import xr_zeros


@dataclass(frozen=True)
class JobConfig:
    dx: float
    epsg: int
    bounds: tuple[float, float, float, float]
    start_date: datetime
    end_date: datetime
    time_frequency_years: int
    bands: list[str]
    ee_path: str
    varname: str
    chunk_size: int
    ee_threads: int

    @property
    def crs(self) -> str:
        return f"epsg:{self.epsg}"

    @property
    def geobox(self) -> GeoBox:
        return GeoBox.from_bbox(self.bounds, crs=self.crs, resolution=self.dx)

    @property
    def chunk_shape(self) -> tuple[int, int]:
        return (self.chunk_size, self.chunk_size)

    @property
    def tiles(self) -> GeoboxTiles:
        return GeoboxTiles(self.geobox, self.chunk_shape)

    @property
    def num_tiles(self) -> int:
        tiles = self.tiles
        return tiles.shape[0] * tiles.shape[1]

    @property
    def time_data(self) -> pd.DatetimeIndex:
        return pd.date_range(
            start=self.start_date,
            end=self.end_date,
            freq=f"{self.time_frequency_years}Y",
        )

    @property
    def num_jobs(self) -> int:
        # not exact; some of the tiles are over ocean and won't generate jobs
        return len(self.time_data) * self.num_tiles

    def create_dataset_schema(self, storage) -> None:
        storage.initialize()

        big_ds = (
            xr_zeros(self.geobox, chunks=-1, dtype="int16")
            .expand_dims(
                {
                    "band": self.bands,
                    "time": self.time_data,
                }
            )
            .transpose(..., "band")
        ).to_dataset(name=self.varname)
        # fill with na value
        # big_ds = big_ds - 32768

        big_ds.attrs["title"] = "HM data cube"

        lon_encoding = optimize_coord_encoding(big_ds.longitude.values, self.dx)
        lat_encoding = optimize_coord_encoding(big_ds.latitude.values, -self.dx)
        encoding = {
            "longitude": {"chunks": big_ds.longitude.shape, **lon_encoding},
            "latitude": {"chunks": big_ds.latitude.shape, **lat_encoding},
            "time": {
                "chunks": big_ds.time.shape,
                "compressor": zarr.Blosc(cname="zstd"),
            },
            "hm": {
                "chunks": (1,) + self.chunk_shape + (len(self.bands),),
                "compressor": zarr.Blosc(cname="zstd"),
                # workaround to create a fill value for the underlying zarr array
                # since Xarray doesn't let us specify one explicitly
                "_FillValue": -32768,
                "dtype": "int16",
            },
        }

        print(big_ds)
        big_ds.to_zarr(
            storage.get_zarr_store(),
            encoding=encoding,
            compute=False,
            zarr_version=storage.zarr_version,
        )
        storage.commit("Wrote initial dataset schema")

    def generate_jobs(
        self, limit: int = 0
    ) -> Generator["ChunkProcessingJob", None, None]:
        count = 0
        for idx in self.tiles._all_tiles():
            tile = self.tiles[idx]
            bbox = tile.boundingbox
            extent = bbox.left, bbox.right, bbox.bottom, bbox.top
            igeoms = list(LAND.intersecting_geometries(extent))

            is_land = len(igeoms) > 0

            if is_land:
                for date in self.time_data:
                    yield ChunkProcessingJob(self, tile_index=idx, year=date.year)
                    count += 1
                    if limit and count >= limit:
                        return


@dataclass(frozen=True)
class ChunkProcessingResult:
    success: bool
    num_scenes: int
    start_time: float
    search_duration: float
    load_duration: float
    write_duration: float
    region: str | None
    cloud_provider: str | None


@dataclass(frozen=True)
class ChunkProcessingJob:
    config: JobConfig
    tile_index: tuple[int, int]
    year: int

    def process(
        self,
        target_array: zarr.Array,
        debug: bool = False,
    ) -> "ChunkProcessingResult":
        start_time = time()

        warnings.filterwarnings("ignore")  # suppress warnings from rasterio

        if debug:
            logger = logging.getLogger("arraylake")
            logger.setLevel(logging.DEBUG)
            stderr_handler = logging.StreamHandler(sys.stderr)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            stderr_handler.setFormatter(formatter)
            logger.addHandler(stderr_handler)

        geobox = self.config.tiles[self.tile_index]

        start_date = datetime(self.year, 1, 1)
        next_year = self.year + self.config.time_frequency_years
        end_date = datetime(next_year, 1, 1) - timedelta(days=1)

        tic1 = perf_counter()

        ic = ee.ImageCollection(self.config.ee_path).filterDate(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )
        # pad with 1 pixel to avoid edge effects
        bbox = ee.Geometry.Rectangle(
            geobox.boundingbox[0] + self.config.dx,
            geobox.boundingbox[1] + self.config.dx,
            geobox.boundingbox[2] + self.config.dx,
            geobox.boundingbox[3] + self.config.dx,
        )
        # create a template xarray as we are not guaranteed
        # that the result from gee will be exactly the same coords
        # as the geobox
        template = xr_zeros(geobox, chunks=-1, dtype="int16")

        ic_len = ic.size().getInfo()
        tic2 = perf_counter()

        if ic_len == 0:
            return ChunkProcessingResult(
                success=False,
                num_scenes=0,
                start_time=start_time,
                search_duration=tic2 - tic1,
                load_duration=0,
                write_duration=0,
                region=os.environ.get("MODAL_REGION", None),
                cloud_provider=os.environ.get("MODAL_CLOUD_PROVIDER", None),
            )
        # TODO: optimize the request size
        # Requests are limited to 48MB in uncompressed data per request
        # computed as the product of the request dimensions in pixels
        # the number of image bands requested,
        # and the number of bytes per pixel for each band.
        # Requests are also limited to at most 32K pixels
        # in either dimension and at most 1024 bands.
        # Requests exceeding these limits will result
        # in an error code of 400 (BAD_REQUEST).
        ds = xr.open_dataset(
            ic,
            engine="ee",
            crs="EPSG:4326",
            scale=self.config.dx,
            geometry=bbox,
            executor_kwargs={"max_workers": self.config.ee_threads},
        )
        ds = ds.median(dim="time")

        # interpolate to the template
        ds = ds.interp_like(template, method="nearest", assume_sorted=True)

        # todo: filter bands
        ds = ds.to_dataarray(dim="band")
        ds = ds * 10000
        ds = ds.fillna(-32768).astype("int16").transpose(..., "band")

        # oversubscribe the thread pool to saturate IO
        # make sure we are using the threaded scheduler and not a cluster (in Coiled)
        #      with dask.config.set(pool=ThreadPoolExecutor(16), scheduler="threads"):
        raw_data = ds.values

        tic3 = perf_counter()

        # target_array = zarr.open(repo.store, path=varname)

        xy_slice = tuple(
            slice(
                cs * ci
                if cs == self.config.chunk_shape[axis]
                else (self.config.chunk_shape[axis] * ci),
                cs * (ci + 1)
                if cs == self.config.chunk_shape[axis]
                else (self.config.chunk_shape[axis] * ci) + cs,
            )
            for cs, ci, axis in zip(geobox.shape, self.tile_index, range(2))
        )

        # not writing with xarray, so have to reverse engineer the time index
        time_index = (
            self.year - self.config.start_date.year
        ) // self.config.time_frequency_years

        # all elements of the selector need to be slices
        # https://github.com/zarr-developers/zarr-python/issues/1730
        target_slice = (slice(time_index, time_index + 1),) + xy_slice

        # todo figure out why the array needs to be rotated
        raw_data = np.rot90(raw_data, k=1, axes=(0, 1))

        # need to expand out the time dimension
        raw_data = raw_data[None, ...]
        # raw_data = einops.rearrange(raw_data, 'a b c d -> a c b d')
        target_array[target_slice] = raw_data

        tic4 = perf_counter()

        return ChunkProcessingResult(
            success=True,
            num_scenes=ic_len,
            start_time=start_time,
            search_duration=tic2 - tic1,
            load_duration=tic3 - tic2,
            write_duration=tic4 - tic3,
            region=os.environ.get("MODAL_REGION", None),
            cloud_provider=os.environ.get("MODAL_CLOUD_PROVIDER", None),
        )


def optimize_coord_encoding(values, dx):
    dx_all = np.diff(values)
    # dx = dx_all[0]
    np.testing.assert_allclose(dx_all, dx), "must be regularly spaced"

    offset_codec = zarr.FixedScaleOffset(
        offset=values[0], scale=1 / dx, dtype=values.dtype, astype="i8"
    )
    delta_codec = zarr.Delta("i8", "i2")
    compressor = zarr.Blosc(cname="zstd")

    enc0 = offset_codec.encode(values)
    # everything should be offset by 1 at this point
    np.testing.assert_equal(np.unique(np.diff(enc0)), [1])
    enc1 = delta_codec.encode(enc0)
    # now we should be able to compress the shit out of this
    enc2 = compressor.encode(enc1)
    decoded = offset_codec.decode(delta_codec.decode(compressor.decode(enc2)))

    # will produce numerical precision differences
    # np.testing.assert_equal(values, decoded)
    np.testing.assert_allclose(values, decoded)

    return {"compressor": compressor, "filters": (offset_codec, delta_codec)}


def save_output_log(results: list[ChunkProcessingResult], fname: str) -> None:
    fields = (
        "success",
        "num_scenes",
        "start_time",
        "search_duration",
        "load_duration",
        "write_duration",
        "region",
        "cloud_provider",
    )

    df = pd.DataFrame(
        [[getattr(r, f) for f in fields] for r in results if r is not None],
        columns=fields,
    )
    df.to_csv(fname, index=False)
