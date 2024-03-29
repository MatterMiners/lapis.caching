from functools import partial

import logging.handlers

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.caching.connection import Connection
from lapis.drone import Drone
from lapis.job_io.htcondor import htcondor_job_reader

from lapis.pool import StaticPool
from lapis.pool_io.htcondor import htcondor_pool_reader
from lapis.job_io.swf import swf_job_reader
from lapis.caching.storageelement import FileBasedHitrateStorage
from lapis.storage_io.storage import (
    storage_reader,
    storage_reader_filebased_hitrate_caching,
)

from lapis.scheduler import CondorClassadJobScheduler

from lapis.simulator import Simulator

from lapis.monitor.core import LoggingUDPSocketHandler
from lapis.monitor.timefilter import SimulationTimeFilter

from time import time

pre_job_rank_default = "0"

machine_ad_defaults = """
    requirements = target.requestcpus <= my.cpus
    rank = 1
    """.strip()

job_ad_defaults = """
    requirements = my.requestcpus <= target.cpus && my.requestmemory <= target.memory
    rank = 0
    """.strip()

last_step = 0

job_import_mapper = {"htcondor": htcondor_job_reader, "swf": swf_job_reader}

pool_import_mapper = {"htcondor": htcondor_pool_reader}

storage_import_mapper = {
    "standard": storage_reader,
    "filehitrate": storage_reader_filebased_hitrate_caching,
}


def create_pool_in_simulator(
    simulator, pool_input, pool_reader, pool_type, connection, controller=None
):
    for pool in pool_reader(
        iterable=pool_input,
        pool_type=pool_type,
        make_drone=partial(Drone, simulator.job_scheduler),
        connection=connection,
    ):
        simulator.pools.append(pool)
        if controller:
            simulator.controllers.append(controller(target=pool, rate=1))


def ini_and_run(
    job_file,
    pool_files,
    storage_file,
    storage_type,
    log_file="test_{}.log".format(time()),
    remote_throughput=1.0,
    seed=1234,
    until=None,
    calculation_efficiency=1.0,
    log_telegraf=False,
    pre_job_rank=pre_job_rank_default,
    machine_ads=machine_ad_defaults,
    job_ads=job_ad_defaults,
    additional_identifier=None,
):
    # ini logging to file
    monitoring_logger = logging.getLogger()
    monitoring_logger.setLevel(logging.DEBUG)
    time_filter = SimulationTimeFilter()
    monitoring_logger.addFilter(time_filter)
    streamHandler = logging.StreamHandler(stream=open(log_file, "w"))
    streamHandler.setFormatter(JsonFormatter())
    monitoring_logger.addHandler(streamHandler)

    if log_telegraf:
        telegrafHandler = LoggingUDPSocketHandler(
            "localhost", logging.handlers.DEFAULT_UDP_LOGGING_PORT
        )
        telegrafHandler.setFormatter(LineProtocolFormatter(resolution=1))
        monitoring_logger.addHandler(telegrafHandler)

    # ini simulation
    print("starting static environment")
    simulator = Simulator(seed=seed)
    file_type = "htcondor"
    file = job_file
    simulator.create_job_generator(
        job_input=open(file, "r"),
        job_reader=partial(
            job_import_mapper[file_type], calculation_efficiency=calculation_efficiency
        ),
    )

    print(
        "scheduler configuration: \n "
        "\tpre job rank: {} \n\n"
        "\tmachine classad:\n \t{}\n\n"
        "\tjob classad: {}".format(pre_job_rank, machine_ads, job_ads)
    )

    simulator.job_scheduler = CondorClassadJobScheduler(
        job_queue=simulator.job_queue,
        pre_job_rank=pre_job_rank,
        machine_ad=machine_ads,
        job_ad=job_ads,
    )

    simulator.connection = Connection(
        remote_throughput * 1000 * 1000 * 1000, filebased_caching=False
    )
    dummy_pool_connection = Connection(float("Inf"))
    print("dummy:", dummy_pool_connection.remote_connection.connection)
    with open(storage_file, "r") as storage_file:
        simulator.create_storage(
            storage_input=storage_file,
            storage_content_input=None,
            storage_reader=storage_import_mapper[storage_type],
            storage_type=FileBasedHitrateStorage,
        )

    for pool_file in pool_files:
        with open(pool_file, "r") as pool_file:
            # Attention: dummy_pool_connection is currently not part of
            # monitoring as it is not known within the simulator itself
            # TODO: do you need this in monitoring?
            create_pool_in_simulator(
                simulator=simulator,
                pool_input=pool_file,
                pool_reader=pool_import_mapper["htcondor"],
                pool_type=StaticPool,
                connection=dummy_pool_connection
                if "dummycluster" in pool_file.name
                else simulator.connection,
            )

    simulator.enable_monitoring()

    # run simulation
    simulator.run(until=until)
