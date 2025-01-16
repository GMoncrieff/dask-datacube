import ee
import zarr
from dask.distributed import Client, LocalCluster, Worker
from distributed.diagnostics.plugin import WorkerPlugin
from lib import ChunkProcessingJob, ChunkProcessingResult
from tqdm import tqdm


class Plugin(WorkerPlugin):
    def __init__(self, *args, **kwargs):
        pass  # the constructor is up to you

    def setup(self, worker: Worker):
        ee.Initialize(
            project="hm-30x30", opt_url="https://earthengine-highvolume.googleapis.com"
        )
        pass

    def teardown(self, worker: Worker):
        pass

    def transition(self, key: str, start: str, finish: str, **kwargs):
        pass

    def release_key(
        self, key: str, state: str, cause: str | None, reason: None, report: bool
    ):
        pass


def process_chunk(
    job: ChunkProcessingJob, array: zarr.Array, debug: bool
) -> ChunkProcessingResult | None:
    return job.process(array, debug=debug)


def spawn_dask_jobs(
    jobs: list[ChunkProcessingJob], array: zarr.Array, debug: bool
) -> list[ChunkProcessingResult]:
    cluster = LocalCluster(n_workers=8, threads_per_worker=5)
    client = Client(cluster)

    plugin = Plugin()
    client.register_plugin(plugin)

    # monitor the submission of jobs
    jobs = list(jobs)
    futures = [
        client.submit(process_chunk, job, array, debug=debug, retries=5)
        for job in tqdm(jobs, desc="Submitting jobs")
    ]

    # block until completed. we can use dask dashboard to monitor progress
    results = client.gather(futures)

    # Shut down the client
    client.close()

    # monitor the submission of jobs
    # jobs = list(jobs)
    # ee.Initialize(
    # project='hm-30x30',
    # opt_url='https://earthengine-highvolume.googleapis.com')
    # futures = []
    # for job in tqdm(jobs, desc="Submitting jobs"):
    #    results = process_chunk(job, array, debug=debug)

    return results
