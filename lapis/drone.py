from cobald import interfaces

from usim import time, Scope, instant, Capacities, ResourcesUnavailable
from typing import Optional

from lapis.job import Job
from lapis.file_provider import FileProvider


class ResourcesExceeded(Exception):
    ...


class Drone(interfaces.Pool):
    def __init__(
        self,
        scheduler,
        fileprovider: FileProvider = FileProvider(),
        pool_resources: Optional[dict] = None,
        scheduling_duration: Optional[float] = None,
        ignore_resources: list = None,
        sitename: str = None,
    ):
        """
        :param scheduler:
        :param pool_resources:
        :param scheduling_duration:
        """
        super(Drone, self).__init__()
        self.scheduler = scheduler
        self.fileprovider = fileprovider
        self.sitename = sitename
        self.pool_resources = pool_resources
        self.resources = Capacities(**pool_resources)
        # shadowing requested resources to determine jobs to be killed
        self.used_resources = Capacities(**pool_resources)
        if ignore_resources:
            self._valid_resource_keys = [
                resource
                for resource in self.pool_resources
                if resource not in ignore_resources
            ]
        else:
            self._valid_resource_keys = self.pool_resources.keys()
        self.scheduling_duration = scheduling_duration
        self._supply = 0
        self.jobs = 0
        self._allocation = None
        self._utilisation = None
        self._job_queue = Queue()

    @property
    def theoretical_available_resources(self):
        return dict(self.resources.levels)

    @property
    def available_resources(self):
        return dict(self.used_resources.levels)

    async def run(self):
        from lapis.monitor import sampling_required

        await (time + self.scheduling_duration)
        self._supply = 1
        self.scheduler.register_drone(self)
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
            resources.append(
                getattr(levels, resource_key) / self.pool_resources[resource_key]
            )
        self._allocation = max(resources)
        self._utilisation = min(resources)

    async def shutdown(self):
        from lapis.monitor import sampling_required

        self._supply = 0
        self.scheduler.unregister_drone(self)
        await sampling_required.put(self)  # TODO: introduce state of drone
        await (time + 1)

    async def schedule_job(self, job: Job, kill: bool = False):
        await self._job_queue.put((job, kill))

    async def _run_job(self, job: Job, kill: bool):
        """
        Method manages to start a job in the context of the given drone.
        The job is started independent of available resources. If resources of
        drone are exceeded, the job is killed.

        :param job: the job to start
        :param kill: if True, a job is killed when used resources exceed
                     requested resources
        :return:
        """
        job.drone = self
        async with Scope() as scope:
            from lapis.monitor import sampling_required

            self._utilisation = self._allocation = None

            job_execution = scope.do(job.run(self))
            self.jobs += 1
            try:
                async with self.resources.claim(
                    **job.resources
                ), self.used_resources.claim(**job.used_resources):
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
                    self.scheduler.update_drone(self)
                    await job_execution.done
                    print(
                        "finished job {} on drone {} @ {}".format(
                            repr(job), repr(self), time.now
                        )
                    )
            except ResourcesUnavailable:
                await instant
                job_execution.cancel()
                await instant
            except AssertionError:
                await instant
                job_execution.cancel()
                await instant
            self.jobs -= 1
            await self.scheduler.job_finished(job)
            self._utilisation = self._allocation = None
            self.scheduler.update_drone(self)
            await sampling_required.put(self)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, id(self))
