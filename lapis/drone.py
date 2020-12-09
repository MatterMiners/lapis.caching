from cobald import interfaces

from usim import time, Scope, instant, Capacities, ResourcesUnavailable, Queue
from typing import Optional

from lapis.job import Job
from lapis.caching.connection import Connection

from lapis.monitor.duplicates import DroneStatusCaching


class ResourcesExceeded(Exception):
    ...


class Drone(interfaces.Pool):
    """
    Represents worker nodes in the simulation.
    """

    def __init__(
        self,
        scheduler,
        pool_resources: Optional[dict] = None,
        scheduling_duration: Optional[float] = None,
        ignore_resources: list = None,
        sitename: str = None,
        connection: Connection = None,
        empty: callable = lambda drone: drone.theoretical_available_resources.get(
            "cores", 1
        )
        < 1,
    ):
        """
        Drone initialization

        :param scheduler: scheduler that assigns jobs to the drone
        :param pool_resources: dict of the drone's resources
        :param scheduling_duration: amount of time that passes between the drone's
        start up and it's registration at the scheduler
        :param ignore_resources: dict of the resource keys that are ignored, e.g. "disk"
        :param sitename: identifier, used to determine which caches a drone can use
        :param connection: connection object that holds remote connection and handles file transfers
        :param empty: callable that determines whether the drone is currently running any jobs
        """
        super(Drone, self).__init__()
        self.scheduler = scheduler
        """scheduler that assigns jobs to the drone"""
        self.connection = connection
        """connection object that holds remote connection and handles file transfers"""
        self.sitename = sitename
        """identifies the site the drone belongs to, used to determine which caches a
        drone can use """
        self.pool_resources = pool_resources
        """dict stating the drone's resources"""
        self.resources = Capacities(**pool_resources)
        """available resources, based on the amount of resources requested by
        jobs running on the drone """
        # shadowing requested resources to determine jobs to be killed
        self.used_resources = Capacities(**pool_resources)
        """available resources, based on the amount of resources actually used by
        jobs running on the drone"""

        if ignore_resources:
            self._valid_resource_keys = [
                resource
                for resource in self.pool_resources
                if resource not in ignore_resources
            ]
        else:
            self._valid_resource_keys = self.pool_resources.keys()
        self.scheduling_duration = scheduling_duration
        """amount of time that passes between the drone's
        start up and it's registration at the scheduler"""
        self._supply = 0
        self.jobs = 0
        """number of jobs running on the drone"""
        self._allocation = None
        self._utilisation = None
        self._job_queue = Queue()
        self._empty = empty
        """method that is used to determine whether a drone is empty"""

        # caching-related
        self.jobs_with_cached_data = 0
        """amount of jobs that currently run on the drone and that could read from
        the cache"""
        self.cached_data = 0
        """used during scheduling, calculated for each job, is assigned the
        expectation value for the amount of cached data that is available to the
        drone"""

    def empty(self):
        """
        Checks whether there are any jobs running on this drone

        :return: true if no jobs are running on this drone, false else
        """
        return self._empty(self)

    @property
    def theoretical_available_resources(self):
        """
        Returns the amount of resources of the drone that were available if all jobs
        used exactly the amount of resources they requested

        :return: dictionary of theoretically available resources
        """
        return dict(self.resources.levels)

    @property
    def available_resources(self):
        """
        Returns the amount of resources of the drone that are available based on the
        amount of resources the running jobs actually use.

        :return: dictionary of available resources
        """
        return dict(self.used_resources.levels)

    async def run(self):
        """
        Handles the drone's activity during simulation. Upon execution the drone
        registers itself at the scheduler and once jobs are scheduled to the drone's
        job queue, these jobs are executed. Starting jobs via a job queue was
        introduced to avoid errors in resource allocation and monitoring.
        """
        from lapis.monitor import sampling_required

        await (time + self.scheduling_duration)
        self._supply = 1
        self.scheduler.register_drone(self)
        await sampling_required.put(
            DroneStatusCaching(
                repr(self),
                self.pool_resources["cores"],
                self.theoretical_available_resources["cores"],
                self.jobs_with_cached_data,
            )
        )
        await sampling_required.put(self)
        async with Scope() as scope:
            async for job, kill in self._job_queue:
                scope.do(self._run_job(job=job, kill=kill))

    @property
    def supply(self) -> float:
        return self._supply

    @property
    def demand(self) -> float:
        return 1

    @demand.setter
    def demand(self, value: float):
        pass  # demand is always defined as 1

    @property
    def utilisation(self) -> float:
        if self._utilisation is None:
            self._init_allocation_and_utilisation()
        return self._utilisation

    @property
    def allocation(self) -> float:
        if self._allocation is None:
            self._init_allocation_and_utilisation()
        return self._allocation

    def _init_allocation_and_utilisation(self):
        levels = self.resources.levels
        resources = []
        for resource_key in self._valid_resource_keys:
            if (
                getattr(levels, resource_key) == 0
                and self.pool_resources[resource_key] == 0
            ):
                pass
            else:
                resources.append(
                    getattr(levels, resource_key) / self.pool_resources[resource_key]
                )
        self._allocation = max(resources)
        self._utilisation = min(resources)

    async def shutdown(self):
        """
        Upon shutdown, the drone unregisters from the scheduler.
        """
        from lapis.monitor import sampling_required

        self._supply = 0
        self.scheduler.unregister_drone(self)
        await sampling_required.put(
            DroneStatusCaching(
                repr(self),
                self.pool_resources["cores"],
                self.theoretical_available_resources["cores"],
                self.jobs_with_cached_data,
            )
        )
        await sampling_required.put(self)  # TODO: introduce state of drone

        await (time + 1)

    async def schedule_job(self, job: Job, kill: bool = False):
        """
        A job is scheduled to a drone by putting it in the drone's job queue.

        :param job: job that was matched to the drone
        :param kill: flag, if true jobs can be killed if they use more resources than they requested
        """
        await self._job_queue.put((job, kill))

    async def _run_job(self, job: Job, kill: bool):
        """
        Method manages to start a job in the context of the given drone.
        The job is started regardless of the available resources. The resource
        allocation takes place after starting the job and the job is killed if the
        drone's overall resources are exceeded. In addition, if the `kill` flag is
        set, jobs are killed if the resources they use exceed the resources they
        requested.
        Then the end of the job's execution is awaited and the drones status
        known to the scheduler is changed.

        :param job: the job to start
        :param kill: if True, a job is killed when used resources exceed
                     requested resources
        """
        job.drone = self
        async with Scope() as scope:
            from lapis.monitor import sampling_required

            self._utilisation = self._allocation = None

            job_execution = scope.do(job.run(self))
            self.jobs += 1
            if job._cached_data:
                self.jobs_with_cached_data += 1
            try:
                async with self.resources.claim(**job.resources):
                    await sampling_required.put(
                        DroneStatusCaching(
                            repr(self),
                            self.pool_resources["cores"],
                            self.theoretical_available_resources["cores"],
                            self.jobs_with_cached_data,
                        )
                    )
                    await sampling_required.put(self)
                    if kill:
                        for resource_key in job.resources:
                            try:
                                if (
                                    job.resources[resource_key]
                                    < job.used_resources[resource_key]
                                ):
                                    await instant
                                    job_execution.cancel()
                                    await instant
                            except KeyError:
                                # check is not relevant if the data is not stored
                                pass
                    # self.scheduler.update_drone(self)
                    await job_execution.done
            except ResourcesUnavailable:
                await instant
                job_execution.cancel()
                await instant
            except AssertionError:
                await instant
                job_execution.cancel()
                await instant
            self.jobs -= 1
            if job._cached_data:
                self.jobs_with_cached_data -= 1

            await self.scheduler.job_finished(job)
            self._utilisation = self._allocation = None
            self.scheduler.update_drone(self)
            await sampling_required.put(self)
            await sampling_required.put(
                DroneStatusCaching(
                    repr(self),
                    self.pool_resources["cores"],
                    self.theoretical_available_resources["cores"],
                    self.jobs_with_cached_data,
                )
            )

    def look_up_cached_data(self, job: Job):
        """
        Determines the amount of the job's input data that is stored in caches the
        drone can access and sets the drone's `cached_data` attribute to the
        resulting value. This quantity can then be used in the job matching process.
        *Pay attention to the fact that the current implementation only works for
        hitrate based caching and that while KeyErrors should not occur due to the
        way the method is called, KeyErrors are not handled here.*

        :param job:
        """
        cached_data = 0
        caches = self.connection.storages.get(self.sitename, None)
        if caches:
            if job.requested_inputfiles:
                cached_data = sum(
                    [
                        filespecs["hitrates"].get(cache.sitename, 0)
                        * filespecs["filesize"]
                        for cache in caches
                        for filespecs in job.requested_inputfiles.values()
                    ]
                )
        self.cached_data = cached_data

    def __repr__(self):
        return "<%s: %s %s>" % (self.__class__.__name__, id(self), self.sitename)
