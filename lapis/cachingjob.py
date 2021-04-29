from typing import Optional, TYPE_CHECKING
from usim import time, Scope, instant
from usim import CancelTask

from lapis.monitor.core import sampling_required
from lapis.job import Job

if TYPE_CHECKING:
    from lapis.drone import Drone


class CachingJob(Job):
    """
    Objects of this class represent jobs. The job is described from the batch
    system's viewpoint by the following attributes:

    :param resources: information about the resources requested by the job
    :param used_resources: information about the resources used by the job
    :param walltime: the job's runtime, in reality as well as in the simulation
    :param requested_walltime: walltime requested by the job
    :param cputime: the cumulated amount of time the used CPU(s) was (were) active
    during the job's execution
    :param queue_date: time when the job was submitted to the simulated job queue
    _success: represents whether the job was run successfully

    In addition, functionality allowing the simulation of data transfers is provided. In
    this case, the job attributes have to be extended by information about the job's
    input data. In this case the job's runtime is recalculated if the job processes
    input data and is executed on resources with access to caches. In this case data
    transfer and processing are assumed to be done in parallel. This is a valid
    assumption if the input data are divided into blocks, transferred
    throughout the job's runtime and if already transferred data blocks are processed
    while other blocks are fetched. If the job's overall runtime is long and if the
    data set was transferred in a large number of blocks, the job's runtime (walltime)
    can be recalculated using max(calculation time, transfer time).

    :param requested_inputfiles: information about the input files requested by a job
    :param used_inputfiles: information about the input files actually read by a job
    :param _total_input_data: data volume of used_inputfiles, amount of data
    processed by the job
    :param _original_walltime: the job's walltime as the simulation's input
    states. Is stored for monitoring purposes as the job's walltime can be
    altered
    :param _calculation_time: the calculation time represents the time needed to
    process the job's input data
    :param calculation_efficiency: represents the efficiency of calculations
    performed on the job's input data. Default = 1.0. Can be modified to take
    programmatical insufficiencies into account.
    :param _transfer_time: the transfer time represents the time needed to
    transfer the job's input data.

    As the simulation of data transfers is used to simulate and study caching,
    the following metadata are introduced and used for monitoring purposes.
    :param _read_from_cache: true if job read data from cache
    :param _cached_data: the amount of input data that is currently cached
    :param failed_matches: number of times a match of this job to a resource was
    rejected (see scheduler for details)
    :param cache_probability: the averaged probability that all data are cached
    (sum(filesize * hitrate = probability that file is cached) / sum(filesize))
    :param expectation_cached_data: the expectation value for the amount of
    cached data (sum(filesize * hitrate))
    """

    __slots__ = (
        "inputfiles",
        "used_inputfiles",
        "calculation_efficiency",
        "__weakref__",
        "_read_from_cache",
        "_cached_data",
        "_total_input_data",
        "_original_walltime",
        "_transfer_time",
        "failed_matches",
        "cputime",
        "cache_probability",
        "expectation_cached_data",
    )

    def __init__(
        self,
        resources: dict,
        used_resources: dict,
        in_queue_since: float = 0,
        queue_date: float = 0,
        name: Optional[str] = None,
        calculation_efficiency: Optional[float] = 1.0,
    ):
        """
        Initialization of a job

        :param resources: Requested resources of the job, including walltime and
        input data
        :param used_resources: Resource usage of the job, including walltime and
        input data
        :param in_queue_since: Time when job was inserted into the queue of the
                               simulation scheduler
        :param queue_date: Time when job was inserted into queue in real life
        :param name: name of the job
        :param drone: drone  the job is running on
        :param calculation_efficiency: efficiency of the job's calculations,
        can be < 1.0 to account for programmatical insufficiencies
        """
        super().__init__(resources, used_resources, in_queue_since, queue_date, name)

        self.calculation_efficiency = calculation_efficiency
        """efficiency of the job's calculations, can be < 1.0 to account for
        programmatical insufficiencies"""
        # caching-related
        self.inputfiles = resources.pop("inputfiles", None)
        """dict of input files requested by the job and respective file sizes"""
        self.used_inputfiles = used_resources.pop("inputfiles", None)
        """dict of input files read by the job and respective amount of read data"""
        self._read_from_cache = 0
        """flag indicating whether the job read from the cache"""
        self._cached_data = 0
        """expectation value for the amount of data that was read from a cache by
        this job"""
        self._original_walltime = self.walltime
        """stores the jobs original walltime as a reference"""
        self._transfer_time = 0
        """time the job takes only to transfer all input data"""

        # TODO: this try-except is a fix for a unit test, check whether this makes
        #  sense in all use cases
        try:
            self.cputime = self.used_resources["cores"] * self.walltime
            """walltime of the job if the CPU efficiency = 1.0"""
        except KeyError:
            self.cputime = None

        try:
            self._total_input_data = sum(
                [fileinfo["usedsize"] for fileinfo in self.used_inputfiles.values()]
            )
            """total data volume of the job's input files"""
        except AttributeError:
            self._total_input_data = 0

        # TODO: see unit test test_read_with_inputfiles ->  making
        #  information about hitrates obilgatory is actually necessary
        if self._total_input_data:
            self.expectation_cached_data = sum(
                [
                    file["usedsize"] * sum(file["hitrates"].values())
                    for file in self.used_inputfiles.values()
                ]
            )
        else:
            self.expectation_cached_data = 0
            """amount of data that was read from the cache"""

        if self._total_input_data:
            self.cache_probability = sum(
                [
                    file["usedsize"] * sum(file["hitrates"].values())
                    for file in self.used_inputfiles.values()
                ]
            ) / sum([file["usedsize"] for file in self.used_inputfiles.values()])
        else:
            self.cache_probability = 0

        self.failed_matches = 0
        """number of times the job entered the matchmaking process but was not
        scheduled to a drone"""

    @property
    def _calculation_time(self):
        """
        Determines a jobs calculation time based on the jobs CPU time and a
        calculation efficiency representing inefficient programming.

        If a job contains input files and the drone the job runs on has a defined
        remote connection (throughput < Inf) the calculation time is given by
        job's CPU time divided by a configurable `calculation_efficiency` that
        can be set != 1, e.g. to account for programmatic inefficiencies.

        Else, the calculation time remains equal to the job's original `walltime`.
        """
        result = self.walltime
        try:
            if (
                not self.inputfiles
                or self.drone.connection.remote_connection.connection.throughput
                == float("Inf")
            ):
                raise KeyError
            result = (
                self.used_resources["cores"] / self.calculation_efficiency
            ) * self.walltime

        except (KeyError, TypeError):
            pass
        return result

    async def _transfer_inputfiles(self):
        transfer_time = 0
        try:
            (
                transfer_time,
                bytes_from_remote,  # FIXME: include somewhere?
                bytes_from_cache,  # FIXME: include somewhere?
                provides_file,
            ) = await self.drone.connection.transfer_files(
                drone=self.drone, requested_files=self.used_inputfiles
            )
            self._read_from_cache = provides_file
        except AttributeError:
            pass
        print("end transfer files ", time.now)
        self._transfer_time = transfer_time

    async def run(self, drone: "Drone"):
        """
        Handles the job's execution.
        The job's runtime is given by max(calculation time, transfer time).
        The calculation time is determined by `_calculate`, the transfer time by
        `_transfer_inputfiles`.
        The job will be executed successfully unless the selected drone does not
        provide enough resources, is unavailable or an exception occurs.

        :param drone: the drone object the job was allocated to and is executed in
        """
        assert drone, "Jobs cannot run without a drone being assigned"
        self.drone = drone
        self.in_queue_until = time.now
        self._success = None
        await sampling_required.put(self)
        try:

            start = time.now
            print(start)
            async with Scope() as scope:
                await instant
                scope.do(self._transfer_inputfiles())
                await (time + self._calculation_time)
        except CancelTask:
            print("CancelTask")
            # self.drone = None
            self._success = False
            # await sampling_required.put(self)
            # TODO: in_queue_until is still set
        except BaseException:
            # self.drone = None
            self._success = False
            await sampling_required.put(self)
            # TODO: in_queue_until is still set
            raise
        else:
            self.walltime = time.now - start
            self._success = True
            await sampling_required.put(self)
