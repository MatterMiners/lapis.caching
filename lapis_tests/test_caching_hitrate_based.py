from usim import time
from tempfile import NamedTemporaryFile
import json

from lapis_tests import via_usim, DummyDrone, DummyJob
from lapis.caching.connection import Connection
from lapis.caching.storageelement import FileBasedHitrateStorage, HitrateStorage
from lapis.storage_io.storage import storage_reader
from lapis.caching.files import RequestedFile
from lapis.simulator import Simulator
from lapis.job_io.htcondor import htcondor_job_reader
from lapis.pool import StaticPool
from lapis.pool_io.htcondor import htcondor_pool_reader
from lapis.scheduler import CondorClassadJobScheduler

conversion_GB_to_B = 1000 * 1000 * 1000


class TestHitrateCaching(object):
    def test_hitratestorage(self):
        size = 1000
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        requested_file = RequestedFile(filename="testfile", filesize=100)
        looked_up_file = hitratestorage.find(requested_file, job_repr=None)

        assert size == hitratestorage.available
        assert 0 == hitratestorage.used
        assert 100 == looked_up_file.cached_filesize
        assert hitratestorage == looked_up_file.storage

    def test_add_storage_to_connection(self):
        throughput = 10
        size = 1000
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        connection = Connection(throughput=throughput)
        connection.add_storage_element(hitratestorage)
        assert hitratestorage in connection.storages[hitratestorage.sitename]

    def test_determine_inputfile_source(self):
        throughput = 10
        size = 1000
        requested_file = RequestedFile(filename="testfile", filesize=100)
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        connection = Connection(throughput=throughput)
        connection.add_storage_element(hitratestorage)
        cache = connection._determine_inputfile_source(
            requested_file=requested_file, dronesite=None
        )
        assert cache is hitratestorage

    @via_usim
    async def test_stream_file(self):
        throughput = 10
        size = 1000
        requested_file = RequestedFile(
            filename="testfile", filesize=100 * conversion_GB_to_B
        )
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        # does not transfer from cache but from remote storage as there are no files
        # in the HitrateStorage
        connection = Connection(throughput=throughput)
        connection.add_storage_element(hitratestorage)
        assert 0 == time.now
        await connection.stream_file(requested_file=requested_file, dronesite=None)
        assert 5 == time.now

    @via_usim
    async def test_single_transfer_files(self):
        throughput = 10
        size = 1000
        drone = DummyDrone(throughput)
        job = DummyJob(True)
        requested_files = dict(
            test=dict(usedsize=100 * conversion_GB_to_B, hitrates={drone.sitename: 1.0})
        )
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        # does not transfer from cache but from remote storage as there are no files
        # in the HitrateStorage
        drone.connection.add_storage_element(hitratestorage)
        stream_time = await drone.connection.transfer_files(
            drone=drone, requested_files=requested_files, job_repr=job
        )

        assert time.now == 5
        assert stream_time == 5

    @via_usim
    async def test_simultaneous_transfer(self):
        throughput = 10
        size = 1000
        drone = DummyDrone(throughput)
        job = DummyJob(True)
        requested_files = dict(
            test1=dict(
                usedsize=100 * conversion_GB_to_B, hitrates={drone.sitename: 1.0}
            ),
            test2=dict(
                usedsize=200 * conversion_GB_to_B, hitrates={drone.sitename: 1.0}
            ),
        )
        hitratestorage = HitrateStorage(hitrate=0.5, size=size, files={})
        drone.connection.add_storage_element(hitratestorage)
        # does not transfer from cache but from remote storage as there are no files
        # in the HitrateStorage
        stream_time = await drone.connection.transfer_files(
            drone=drone, requested_files=requested_files, job_repr=job
        )
        assert time.now == 15
        assert stream_time == 15

    def test_full_simulation_with_hitratebased_caching(self):
        with NamedTemporaryFile(suffix=".csv") as machine_config, NamedTemporaryFile(
            suffix=".csv"
        ) as storage_config, NamedTemporaryFile(suffix=".json") as job_config:
            with open(machine_config.name, "w") as write_stream:
                write_stream.write(
                    "TotalSlotCPUs TotalSlotDisk TotalSlotMemory Count sitename \n"
                    "1 44624348.0 4000 1 mysite"
                )
            with open(job_config.name, "w") as write_stream:
                job_description = [
                    {
                        "RequestCpus": 1,
                        "RequestWalltime": 60,
                        "RequestMemory": 2000,
                        "RequestDisk": 6000000,
                        "QDate": 0,
                        "RemoteWallClockTime": 42,
                        "Number of Allocated Processors": 1,
                        "MemoryUsage": 1500,
                        "DiskUsage_RAW": 41898,
                        "RemoteSysCpu": 40,
                        "RemoteUserCpu": 2,
                        "Inputfiles": {
                            "a.root": {
                                "filesize": 5,
                                "usedsize": 5,
                                "hitrates": {"mysite": 1.0},
                            },
                            "b.root": {
                                "filesize": 5,
                                "usedsize": 5,
                                "hitrates": {"mysite": 0.0},
                            },
                        },
                    }
                ]
                json.dump(job_description, write_stream)
            with open(storage_config.name, "w") as write_stream:
                write_stream.write(
                    "name sitename cachesizeGB throughput_limit \n"
                    "mycache mysite 1000 1.0"
                )

            job_input = open(job_config.name, "r+")
            machine_input = open(machine_config.name, "r+")
            storage_input = open(storage_config.name, "r+")
            storage_content_input = None

            simulator = Simulator()
            simulator.create_job_generator(
                job_input=job_input, job_reader=htcondor_job_reader
            )
            simulator.create_scheduler(scheduler_type=CondorClassadJobScheduler)
            simulator.create_connection_module(
                remote_throughput=0.1, filebased_caching=False
            )
            simulator.create_storage(
                storage_input=storage_input,
                storage_content_input=storage_content_input,
                storage_reader=storage_reader,
                storage_type=FileBasedHitrateStorage,
            )
            simulator.create_pools(
                pool_input=machine_input,
                pool_reader=htcondor_pool_reader,
                pool_type=StaticPool,
            )

            simulator.enable_monitoring()
            simulator.run()
            assert 180 == simulator.duration

            job_input.close()
            storage_input.close()
            machine_input.close()
