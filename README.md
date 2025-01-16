# Dask GEE Mosaic

This repo demonstrates how to build a Zarr-based data cube from data on GEE.

**Note**: This repo is for demonstration purposes only. It does not aspire to be a maintained package.
If you want to build on top of it, fork this repo and modify it to your needs.

**License:** Apache 2.0

**Supported Backends**

- Dask

**Supported Storage Locations**

- [Arraylake](https://docs.earthmover.io/) - Earthmover's data lake platform for Zarr data
- Any [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)-compatible cloud storage location (e.g. S3)

## Usage

```
% python src/main.py --help
Usage: main.py [OPTIONS]

Options:
  --start-date [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]
                                  Start date for the data cube. Everything but
                                  year will be ignored.  [required]
  --end-date [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]
                                  End date for the data cube. Everything but
                                  year will be ignored .  [required]
  --bbox <FLOAT FLOAT FLOAT FLOAT>...
                                  Bounding box for the data cube in lat/lon.
                                  (min_lon, min_lat, max_lon, max_lat)
                                  [required]
  --time-frequency-years INTEGER RANGE
                                  Temporal sampling frequency in years.
                                  [1<=x<=10]
  --resolution FLOAT              Spatial resolution in degrees.  [default:
                                  0.009]
  --chunk-size INTEGER            Zarr chunk size for the data cube.
                                  [default: 100]
  --bands TEXT                    Bands to include in the data cube. Must
                                  match band names from odc.stac.load
                                  [default: AA, AG, BU, HI, PO, EX, FR, TI,
                                  NS]
  --ee-path TEXT                  The path to the Earth Engine data.
                                  [default: projects/hm-30x30/assets/output/v2
                                  0240801/HM_change_300]
  --varname TEXT                  The name of the variable to use in the Zarr
                                  data cube.  [default: hm]
  --ee-threads INTEGER            The name of threads per worker for ee
                                  requests.  [default: 1]
  --epsg [4326]                   EPSG for the data cube. Only 4326 is
                                  supported at the moment.  [default: 4326]
  --serverless-backend [dask]     [required]
  --storage-backend [arraylake|fsspec]
                                  [default: arraylake; required]
  --arraylake-repo-name TEXT      Name of the Arraylake repo to use for
                                  storage.
  --fsspec-uri TEXT
  --limit INTEGER                 Limit the number of chunks to process.
  --debug                         Enable debug logging.
  --initialize / --no-initialize  Initialize the Zarr store before processing.
  --help                          Show this message and exit.
```

## Example Usage

Human modification Datacube, store in local zarr

```
python src/main.py --start-date 2000-01-01 --end-date 2010-12-31 --bbox 17.58 -35.00 21.38 -32.23 \
--fsspec-uri "data/hmtest.zarr" \
--storage-backend fsspec --serverless-backend dask
```

