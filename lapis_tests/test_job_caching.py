from usim import time

from lapis.job import Job

from lapis_tests import via_usim, DummyDrone


class TestJobCaching(object):
    @via_usim
    async def test_calculation_time(self):
        self.job = Job(resources={"walltime": 60},
                       used_resources={"walltime": 10, "cores": 0.7})
        self.job.drone = DummyDrone(1)
        starttime = time.now
        await self.job._calculate()
        assert time.now - starttime == 10

        self.job = Job(resources={"walltime": 60, "inputfiles": {"file"}},
                       used_resources={"walltime": 10, "cores": 0.7})
        self.job.drone = DummyDrone(1)
        starttime = time.now
        await self.job._calculate()
        assert time.now - starttime == 7

        self.job = Job(resources={"walltime": 60, "inputfiles": {"file"}},
                       used_resources={"walltime": 10, "cores": 0.7},
                       calculation_efficiency=0.5)
        self.job.drone = DummyDrone(1)
        starttime = time.now
        await self.job._calculate()
        assert time.now - starttime == 14

        self.job = Job(resources={"walltime": 60, "inputfiles": {"file"}},
                       used_resources={"walltime": 10},
                       calculation_efficiency=0.5)
        self.job.drone = DummyDrone(1)
        starttime = time.now
        await self.job._calculate()
        assert time.now - starttime == 10

    @via_usim
    async def test_transfer_time(self):
        conversion_GB_to_B = 1000 * 1000 * 1000
        drone = DummyDrone(1)
        self.job = Job(resources={"walltime": 60,
                                  "inputfiles": {"file": {"usedsize": 20 *conversion_GB_to_B}}},
                       used_resources={"walltime": 10,
                                       "inputfiles": {
                                           "file": {"usedsize": 20 * conversion_GB_to_B,
                                                    "hitrates": {}}}
                                       },
                       calculation_efficiency=1.0)

        self.job.drone = drone
        starttime = time.now
        await self.job._transfer_inputfiles()
        assert time.now - starttime == 20

        self.job = Job(resources={"walltime": 60},
                       used_resources={"walltime": 10},
                       calculation_efficiency=1.0)

        self.job.drone = drone
        starttime = time.now
        await self.job._transfer_inputfiles()
        assert time.now - starttime == 0

        self.job = Job(resources={"walltime": 60,
                                  "inputfiles": {
                                      "file": {"usedsize": 20 *conversion_GB_to_B}}},
                       used_resources={"walltime": 10},
                       calculation_efficiency=1.0)

        self.job.drone = drone
        starttime = time.now
        await self.job._transfer_inputfiles()
        assert time.now - starttime == 0

        self.job = Job(resources={"walltime": 60,
                                  "inputfiles": {
                                      "file": {"usedsize": 20 * conversion_GB_to_B}}},
                       used_resources={"walltime": 10,
                                       "inputfiles": {
                                           "file": {"usedsize": 20 *
                                                                conversion_GB_to_B,
                                                    "hitrates": {}},
                                       }
                                       },
                       calculation_efficiency=1.0)

        self.job.drone = drone
        starttime = time.now
        await self.job._transfer_inputfiles()
        assert time.now - starttime == 20

