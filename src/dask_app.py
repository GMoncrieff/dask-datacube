import ee
import zarr
from dask.distributed import Client, LocalCluster, Worker, performance_report
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

    with performance_report(filename="dask_report.html"):
        # monitor the submission of jobs
        jobs = list(jobs)
        results = []
        batch_size = 100
    
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i + batch_size]
            futures = [
                client.submit(process_chunk, job, array, debug=debug, retries=5)
                for job in tqdm(batch, desc=f"Submitting jobs batch {i//batch_size + 1}")
            ]
    
            # block until batch completed
            batch_results = client.gather(futures)
            results.extend(batch_results)

    # Shut down the client
    client.close()

    return results