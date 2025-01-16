from datetime import datetime

import click
import zarr
from dask_app import spawn_dask_jobs
from lib import JobConfig, save_output_log
from storage import ArraylakeStorage, ZarrFSSpecStorage


@click.command()
@click.option(
    "--start-date",
    type=click.DateTime(),
    required=True,
    help="Start date for the data cube. Everything but year will be ignored.",
)
@click.option(
    "--end-date",
    type=click.DateTime(),
    required=True,
    help="End date for the data cube. Everything but year will be ignored .",
)
@click.option(
    "--bbox",
    required=True,
    type=click.Tuple([float, float, float, float]),
    help="Bounding box for the data cube in lat/lon. "
    "(min_lon, min_lat, max_lon, max_lat)",
)
@click.option(
    "--time-frequency-years",
    default=5,
    type=click.IntRange(1, 10),
    help="Temporal sampling frequency in years.",
)
@click.option(
    "--resolution",
    type=float,
    default=0.009,
    show_default=True,
    help="Spatial resolution in degrees.",
)
@click.option(
    "--chunk-size",
    type=int,
    default=100,
    show_default=True,
    help="Zarr chunk size for the data cube.",
)
@click.option(
    "--bands",
    multiple=True,
    default=["AA", "AG", "BU", "HI", "PO", "EX", "FR", "TI", "NS"],
    show_default=True,
    help="Bands to include in the data cube. Must match band names from odc.stac.load",
)
@click.option(
    "--ee-path",
    default="projects/hm-30x30/assets/output/v20240801/HM_change_300",
    show_default=True,
    help="The path to the Earth Engine data.",
)
@click.option(
    "--varname",
    default="hm",
    show_default=True,
    help="The name of the variable to use in the Zarr data cube.",
)
@click.option(
    "--ee-threads",
    default=1,
    show_default=True,
    help="The name of threads per worker for ee requests.",
)
@click.option(
    "--epsg",
    type=click.Choice(["4326"]),
    default="4326",
    show_default=True,
    help="EPSG for the data cube. Only 4326 is supported at the moment.",
)
@click.option(
    "--serverless-backend",
    required=True,
    type=click.Choice(["dask"]),
)
@click.option(
    "--storage-backend",
    required=True,
    type=click.Choice(["arraylake", "fsspec"]),
    default="arraylake",
    show_default=True,
)
@click.option(
    "--arraylake-repo-name",
    help="Name of the Arraylake repo to use for storage.",
)
@click.option("--fsspec-uri")
@click.option(
    "--limit",
    type=int,
    help="Limit the number of chunks to process.",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable debug logging.",
)
@click.option(
    "--initialize/--no-initialize",
    is_flag=True,
    default=True,
    help="Initialize the Zarr store before processing.",
)
def main(
    start_date: datetime,
    end_date: datetime,
    bbox: tuple[float, float, float, float],
    time_frequency_years: int,
    resolution: float,
    chunk_size: int,
    ee_threads: int,
    ee_path: str,
    bands: list[str],
    varname: str,
    epsg: str,
    serverless_backend: str,
    storage_backend: str,
    arraylake_repo_name: str | None,
    fsspec_uri: str | None,
    limit: int | None,
    initialize: bool,
    debug: bool,
):
    job_config = JobConfig(
        dx=resolution,
        epsg=int(epsg),
        bounds=bbox,
        ee_path=ee_path,
        start_date=start_date,
        end_date=end_date,
        time_frequency_years=time_frequency_years,
        bands=bands,
        varname=varname,
        chunk_size=chunk_size,
        ee_threads=ee_threads,
    )

    if storage_backend == "arraylake":
        storage = ArraylakeStorage(repo_name=arraylake_repo_name)
    elif storage_backend == "fsspec":
        storage = ZarrFSSpecStorage(uri=fsspec_uri)

    if initialize:
        job_config.create_dataset_schema(storage)

    with click.progressbar(
        job_config.generate_jobs(limit=(limit or 0)),
        label=f"Computing {job_config.num_tiles} tile intersections with land mask",
        length=job_config.num_jobs,
    ) as job_gen:
        jobs = list(job_gen)

    if serverless_backend == "dask":
        spawn = spawn_dask_jobs
    else:
        raise NotImplementedError

    target_array = zarr.open(storage.get_zarr_store(), path=job_config.varname)

    click.echo(f"Spawning {len(jobs)} jobs")

    with click.progressbar(
        jobs,
        label=f"Spawning {len(jobs)} jobs",
        length=len(jobs),
    ) as jobs_progress:
        # all the work happens here
        results = spawn(jobs_progress, target_array, debug=debug)

    # commit changes only of successful
    storage.commit(f"Processed {len(jobs)} chunks")

    # save logs
    log_fname = f"logs/{int(datetime.now().timestamp())}-{serverless_backend}.csv"
    save_output_log(results, log_fname)


if __name__ == "__main__":
    main()
