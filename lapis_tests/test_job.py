import pytest
from usim import Scope, time

from lapis.drone import Drone
from lapis.cachingjob import CachingJob

from lapis_tests import via_usim, DummyScheduler, DummyDrone
from lapis.caching.connection import Connection


class TestJob(object):
    def test_init(self):
        with pytest.raises(KeyError):
            CachingJob(resources={}, used_resources={})
        with pytest.raises(KeyError):
            CachingJob(resources={"walltime": 100}, used_resources={})
        assert CachingJob(resources={}, used_resources={"walltime": 100})
        with pytest.raises(AssertionError):
            CachingJob(
                resources={}, used_resources={"walltime": 100}, in_queue_since=-5
            )

    def test_name(self):
        name = "test"
        job = CachingJob(resources={}, used_resources={"walltime": 100}, name=name)
        assert job.name == name
        assert repr(job) == "<CachingJob: %s>" % name
        job = CachingJob(resources={}, used_resources={"walltime": 100})
        assert job.name == id(job)
        assert repr(job) == "<CachingJob: %s>" % id(job)

    @via_usim
    async def test_run_job(self):
        drone = DummyDrone()
        job = CachingJob(resources={"walltime": 50}, used_resources={"walltime": 10})
        assert float("inf") == job.waiting_time
        async with Scope() as scope:
            scope.do(job.run(drone))
        assert 10 == time
        assert 0 == job.waiting_time
        assert job.successful

    @via_usim
    async def test_job_in_drone(self):
        scheduler = DummyScheduler()
        job = CachingJob(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        drone = Drone(
            scheduler=scheduler,
            pool_resources={"cores": 1, "memory": 1},
            scheduling_duration=0,
            connection=Connection(throughput=1),
        )
        async with Scope() as scope:
            scope.do(drone.run(), volatile=True)
            scope.do(drone.schedule_job(job=job))
            await (
                scheduler.statistics._available
                == scheduler.statistics.resource_type(job_succeeded=1)
            )
        assert 10 == time.now
        assert 0 == job.waiting_time
        assert job.successful

    @via_usim
    async def test_nonmatching_job_in_drone(self):
        scheduler = DummyScheduler()
        job = CachingJob(
            resources={"walltime": 50, "cores": 2, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        drone = Drone(
            scheduler=scheduler,
            pool_resources={"cores": 1, "memory": 1},
            scheduling_duration=0,
        )
        async with Scope() as scope:
            scope.do(drone.run(), volatile=True)
            scope.do(drone.schedule_job(job=job))
            await (
                scheduler.statistics._available
                == scheduler.statistics.resource_type(job_failed=1)
            )
        assert 0 == time
        assert not job.successful
        assert 0 == job.waiting_time

    @via_usim
    async def test_two_nonmatching_jobs(self):
        scheduler = DummyScheduler()
        job_one = CachingJob(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        job_two = CachingJob(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        drone = Drone(
            scheduler=scheduler,
            pool_resources={"cores": 1, "memory": 1},
            scheduling_duration=0,
        )
        async with Scope() as scope:
            scope.do(drone.run(), volatile=True)
            scope.do(drone.schedule_job(job=job_one))
            scope.do(drone.schedule_job(job=job_two))
            await (
                scheduler.statistics._available
                == scheduler.statistics.resource_type(job_succeeded=1, job_failed=1)
            )
        assert 10 == time
        assert job_one.successful
        assert not job_two.successful
        assert 0 == job_one.waiting_time
        assert 0 == job_two.waiting_time

    @via_usim
    async def test_two_matching_jobs(self):
        scheduler = DummyScheduler()
        job_one = CachingJob(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        job_two = CachingJob(
            resources={"walltime": 50, "cores": 1, "memory": 1},
            used_resources={"walltime": 10, "cores": 1, "memory": 1},
        )
        drone = Drone(
            scheduler=scheduler,
            pool_resources={"cores": 2, "memory": 2},
            scheduling_duration=0,
        )
        async with Scope() as scope:
            scope.do(drone.run(), volatile=True)
            scope.do(drone.schedule_job(job=job_one))
            scope.do(drone.schedule_job(job=job_two))
            await (
                scheduler.statistics._available
                == scheduler.statistics.resource_type(job_succeeded=2)
            )
        assert 10 == time
        assert job_one.successful
        assert job_two.successful
        assert 0 == job_one.waiting_time
        assert 0 == job_two.waiting_time
