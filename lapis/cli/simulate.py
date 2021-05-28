from functools import partial

import click
import logging.handlers

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.controller import SimulatedLinearController
from lapis.job_io.htcondor import htcondor_job_reader
from lapis.pool import StaticPool, Pool
from lapis.pool_io.htcondor import htcondor_pool_reader
from lapis.job_io.swf import swf_job_reader
from lapis.caching.storageelement import FileBasedHitrateStorage
from lapis.storage_io.storage import (
    storage_reader,
    storage_reader_filebased_hitrate_caching,
)

from lapis.scheduler import CondorJobScheduler, CondorClassadJobScheduler
from lapis.simulator import Simulator

from lapis.monitor.core import LoggingSocketHandler, LoggingUDPSocketHandler
from lapis.monitor.timefilter import SimulationTimeFilter

last_step = 0

job_import_mapper = {"htcondor": htcondor_job_reader, "swf": swf_job_reader}

scheduler_import_mapper = {
    "condor_simplified": CondorJobScheduler,
    "condor_classad": CondorClassadJobScheduler,
}

pool_import_mapper = {"htcondor": htcondor_pool_reader}

storage_import_mapper = {
    "standard": storage_reader,
    "filehitrate": storage_reader_filebased_hitrate_caching,
}

"""Simulation CLI, pay attention to the fact that the random seed is currently set to a
fixed value"""


@click.group()
@click.option("--seed", type=int, default=1234, help="random seed")
@click.option("--until", type=float)
@click.option("--log-tcp", "log_tcp", is_flag=True)
@click.option("--log-file", "log_file", type=click.File("w"))
@click.option("--log-telegraf", "log_telegraf", is_flag=True)
@click.option("--calculation-efficiency", type=float, default=1.0)
@click.pass_context
def cli(ctx, seed, until, log_tcp, log_file, log_telegraf, calculation_efficiency):
    ctx.ensure_object(dict)
    ctx.obj["seed"] = seed
    ctx.obj["until"] = until
    ctx.obj["calculation_efficiency"] = calculation_efficiency
    monitoring_logger = logging.getLogger()
    monitoring_logger.setLevel(logging.DEBUG)
    time_filter = SimulationTimeFilter()
    monitoring_logger.addFilter(time_filter)
    if log_tcp:
        socketHandler = LoggingSocketHandler(
            "localhost", logging.handlers.DEFAULT_TCP_LOGGING_PORT
        )
        socketHandler.setFormatter(JsonFormatter())
        monitoring_logger.addHandler(socketHandler)
    if log_file:
        streamHandler = logging.StreamHandler(stream=log_file)
        streamHandler.setFormatter(JsonFormatter())
        monitoring_logger.addHandler(streamHandler)
    if log_telegraf:
        telegrafHandler = LoggingUDPSocketHandler(
            "localhost", logging.handlers.DEFAULT_UDP_LOGGING_PORT
        )
        telegrafHandler.setFormatter(LineProtocolFormatter(resolution=1))
        monitoring_logger.addHandler(telegrafHandler)


@cli.command()
@click.option(
    "--job-file",
    "job_file",
    type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))),
)
@click.option("--pre-job-rank", "pre_job_rank", type=str, default=None)
@click.option("--machine-ads", "machine_ads", type=str, default=None)
@click.option("--job-ads", "job_ads", type=str, default=None)
@click.option(
    "--scheduler-type",
    "scheduler_type",
    type=click.Choice(list(scheduler_import_mapper.keys())),
)
@click.option(
    "--pool-files",
    "pool_files",
    type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))),
    multiple=True,
    help="Tuple of `(pool_file,pool_file_type)`",
)
@click.option(
    "--storage-files",
    "storage_files",
    type=(
        click.File("r"),
        click.File("r"),
        click.Choice(list(storage_import_mapper.keys())),
    ),
    default=(None, None, None),
    multiple=True,
    help="Tuple of `(storage_file,storage_content_file,storage_type)`",
)
@click.option(
    "--remote-throughput",
    "remote_throughput",
    type=float,
    default=1.0,
    help="Parameter to set the network bandwidth to remote",
)
@click.option(
    "--filebased_caching",
    "filebased_caching",
    is_flag=True,
    help="Flag to set filebased caching on/off",
    default=False,
)
@click.option("--cache-hitrate", "cache_hitrate", type=float, default=None)
@click.pass_context
def static(
    ctx,
    job_file,
    pre_job_rank,
    machine_ads,
    job_ads,
    scheduler_type,
    pool_files,
    storage_files,
    remote_throughput,
    filebased_caching,
    cache_hitrate,
):
    click.echo("starting static environment")
    simulator = Simulator(seed=ctx.obj["seed"])
    infile, file_type = job_file
    simulator.create_job_generator(
        job_input=infile,
        job_reader=partial(
            job_import_mapper[file_type],
            calculation_efficiency=ctx.obj["calculation_efficiency"],
        ),
    )
    click.echo(
        "scheduler configuration: \n "
        "\tscheduler type: {}\n\n"
        "\tpre job rank: {} \n\n"
        "\tmachine classads:\n \t{}\n\n"
        "\tjob classads: {}".format(scheduler_type, pre_job_rank, machine_ads, job_ads)
    )

    if scheduler_import_mapper[scheduler_type] == CondorClassadJobScheduler and any(
        (pre_job_rank, machine_ads, job_ads)
    ):
        simulator.job_scheduler = CondorClassadJobScheduler(
            job_queue=simulator.job_queue,
            pre_job_rank=pre_job_rank,
            machine_ad=machine_ads,
            job_ad=job_ads,
        )
    else:
        simulator.create_scheduler(
            scheduler_type=scheduler_import_mapper[scheduler_type]
        )

    for current_storage_files in storage_files:
        assert all(current_storage_files), "All storage inputs have to be set"
        simulator.create_connection_module(remote_throughput, filebased_caching)
        storage_file, storage_content_file, storage_type = current_storage_files
        simulator.create_storage(
            storage_input=storage_file,
            storage_content_input=storage_content_file,
            storage_reader=storage_import_mapper[storage_type],
            storage_type=FileBasedHitrateStorage,  # TODO: Generalize this
        )
    for current_pool in pool_files:
        pool_file, pool_file_type = current_pool
        if "dummycluster" in pool_file.name:
            simulator.create_connection_module(float("Inf"))
        simulator.create_pools(
            pool_input=pool_file,
            pool_reader=pool_import_mapper[pool_file_type],
            pool_type=StaticPool,
        )
    simulator.enable_monitoring()
    simulator.run(until=ctx.obj["until"])


@cli.command()
@click.option(
    "--job-file",
    "job_file",
    type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))),
)
@click.option("--pre-job-rank", "pre_job_rank", type=str, default=None)
@click.option("--machine-ads", "machine_ads", type=str, default=None)
@click.option("--job-ads", "job_ads", type=str, default=None)
@click.option(
    "--scheduler-type",
    "scheduler_type",
    type=click.Choice(list(scheduler_import_mapper.keys())),
)
@click.option(
    "--pool-files",
    "pool_files",
    type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))),
    multiple=True,
    help="Tuple of `(pool_file,pool_file_type)`",
)
@click.option(
    "--storage-files",
    "storage_files",
    type=(
        click.File("r"),
        click.File("r"),
        click.Choice(list(storage_import_mapper.keys())),
    ),
    default=(None, None, None),
    multiple=True,
    help="Tuple of `(storage_file,storage_content_file,storage_type)`",
)
@click.option(
    "--remote-throughput",
    "remote_throughput",
    type=float,
    default=1.0,
    help="Parameter to set the network bandwidth to remote",
)
@click.option(
    "--filebased_caching",
    "filebased_caching",
    is_flag=True,
    help="Flag to set filebased caching on/off",
    default=False,
)
@click.option("--cache-hitrate", "cache_hitrate", type=float, default=None)
@click.pass_context
def dynamic(
    ctx,
    job_file,
    pre_job_rank,
    machine_ads,
    job_ads,
    scheduler_type,
    pool_files,
    storage_files,
    remote_throughput,
    filebased_caching,
    cache_hitrate,
):
    click.echo("starting dynamic environment")
    simulator = Simulator(seed=ctx.obj["seed"])
    infile, file_type = job_file
    simulator.create_job_generator(
        job_input=infile,
        job_reader=partial(
            job_import_mapper[file_type],
            calculation_efficiency=ctx.obj["calculation_efficiency"],
        ),
    )
    click.echo(
        "scheduler configuration: \n "
        "\tscheduler type: {}\n\n"
        "\tpre job rank: {} \n\n"
        "\tmachine classads:\n \t{}\n\n"
        "\tjob classads: {}".format(scheduler_type, pre_job_rank, machine_ads, job_ads)
    )

    if scheduler_import_mapper[scheduler_type] == CondorClassadJobScheduler and any(
        (pre_job_rank, machine_ads, job_ads)
    ):
        simulator.job_scheduler = CondorClassadJobScheduler(
            job_queue=simulator.job_queue,
            pre_job_rank=pre_job_rank,
            machine_ad=machine_ads,
            job_ad=job_ads,
        )
    else:
        simulator.create_scheduler(
            scheduler_type=scheduler_import_mapper[scheduler_type]
        )

    for current_storage_files in storage_files:
        assert all(current_storage_files), "All storage inputs have to be set"
        simulator.create_connection_module(remote_throughput, filebased_caching)
        storage_file, storage_content_file, storage_type = current_storage_files
        simulator.create_storage(
            storage_input=storage_file,
            storage_content_input=storage_content_file,
            storage_reader=storage_import_mapper[storage_type],
            storage_type=FileBasedHitrateStorage,  # TODO: Generalize this
        )
    for current_pool in pool_files:
        pool_file, pool_file_type = current_pool
        if "dummycluster" in pool_file.name:
            simulator.create_connection_module(float("Inf"))
        simulator.create_pools(
            pool_input=pool_file,
            pool_reader=pool_import_mapper[pool_file_type],
            pool_type=Pool,
            controller=SimulatedLinearController,
        )
    simulator.enable_monitoring()
    simulator.run(until=ctx.obj["until"])


@cli.command()
@click.option(
    "--job-file",
    "job_file",
    type=(click.File("r"), click.Choice(list(job_import_mapper.keys()))),
)
@click.option("--pre-job-rank", "pre_job_rank", type=str, default=None)
@click.option("--machine-ads", "machine_ads", type=str, default=None)
@click.option("--job-ads", "job_ads", type=str, default=None)
@click.option(
    "--scheduler-type",
    "scheduler_type",
    type=click.Choice(list(scheduler_import_mapper.keys())),
)
@click.option(
    "--static-pool-files",
    "static_pool_files",
    type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))),
    multiple=True,
    help="Tuple of `(static_pool_file,static_pool_file_type)`",
)
@click.option(
    "--dynamic-pool-files",
    "dynamic_pool_files",
    type=(click.File("r"), click.Choice(list(pool_import_mapper.keys()))),
    multiple=True,
    help="Tuple of `(dynamic_pool_file,dynamic_pool_file_type)`",
)
@click.option(
    "--storage-files",
    "storage_files",
    type=(
        click.File("r"),
        click.File("r"),
        click.Choice(list(storage_import_mapper.keys())),
    ),
    default=(None, None, None),
    multiple=True,
    help="Tuple of `(storage_file,storage_content_file,storage_type)`",
)
@click.option(
    "--remote-throughput",
    "remote_throughput",
    type=float,
    default=10,
    help="Parameter to set the network bandwidth to remote",
)
@click.option(
    "--filebased_caching",
    "filebased_caching",
    is_flag=True,
    help="Flag to set filebased caching on/off",
    default=False,
)
@click.option("--cache-hitrate", "cache_hitrate", type=float, default=None)
@click.pass_context
def hybrid(
    ctx,
    job_file,
    pre_job_rank,
    machine_ads,
    job_ads,
    scheduler_type,
    static_pool_files,
    dynamic_pool_files,
    storage_files,
    remote_throughput,
    filebased_caching,
    cache_hitrate,
):
    click.echo("starting hybrid environment")
    simulator = Simulator(seed=ctx.obj["seed"])
    infile, file_type = job_file
    simulator.create_job_generator(
        job_input=infile,
        job_reader=partial(
            job_import_mapper[file_type],
            calculation_efficiency=ctx.obj["calculation_efficiency"],
        ),
    )
    click.echo(
        "scheduler configuration: \n "
        "\tscheduler type: {}\n\n"
        "\tpre job rank: {} \n\n"
        "\tmachine classads:\n \t{}\n\n"
        "\tjob classads: {}".format(scheduler_type, pre_job_rank, machine_ads, job_ads)
    )

    if scheduler_import_mapper[scheduler_type] == CondorClassadJobScheduler and any(
        (pre_job_rank, machine_ads, job_ads)
    ):
        simulator.job_scheduler = CondorClassadJobScheduler(
            job_queue=simulator.job_queue,
            pre_job_rank=pre_job_rank,
            machine_ad=machine_ads,
            job_ad=job_ads,
        )
    else:
        simulator.create_scheduler(
            scheduler_type=scheduler_import_mapper[scheduler_type]
        )

    for current_storage_files in storage_files:
        assert all(current_storage_files), "All storage inputs have to be set"
        simulator.create_connection_module(remote_throughput, filebased_caching)
        storage_file, storage_content_file, storage_type = current_storage_files
        simulator.create_storage(
            storage_input=storage_file,
            storage_content_input=storage_content_file,
            storage_reader=storage_import_mapper[storage_type],
            storage_type=FileBasedHitrateStorage,  # TODO: Generalize this
        )
    for current_pool in static_pool_files:
        pool_file, pool_file_type = current_pool
        if "dummycluster" in pool_file.name:
            simulator.create_connection_module(float("Inf"))
        simulator.create_pools(
            pool_input=pool_file,
            pool_reader=pool_import_mapper[pool_file_type],
            pool_type=StaticPool,
        )
    for current_pool in dynamic_pool_files:
        pool_file, pool_file_type = current_pool
        if "dummycluster" in pool_file.name:
            simulator.create_connection_module(float("Inf"))
        simulator.create_pools(
            pool_input=pool_file,
            pool_reader=pool_import_mapper[pool_file_type],
            pool_type=Pool,
            controller=SimulatedLinearController,
        )
    simulator.enable_monitoring()
    simulator.run(until=ctx.obj["until"])


if __name__ == "__main__":
    cli()
