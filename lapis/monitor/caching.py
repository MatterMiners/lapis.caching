import logging

from typing import NamedTuple, List, Dict

from cobald.monitor.format_json import JsonFormatter
from cobald.monitor.format_line import LineProtocolFormatter

from lapis.drone import Drone
from lapis.monitor.core import (
    LoggingSocketHandler,
    LoggingUDPSocketHandler,
    SIMULATION_START,
)
from lapis.caching.storageelement import StorageElement
from lapis.caching.monitoredpipe import MonitoredPipe, MonitoredPipeInfo

import time as pytime

from lapis.cachingjob import CachingJob


class HitrateInfo(NamedTuple):
    hitrate: float
    volume: float
    provides_file: int


class SimulationInfo(NamedTuple):
    input: list
    identifier: str


def simulation_id(simulationinfo) -> list:
    results = [
        {
            "input": str(simulationinfo.input),
            "id": simulationinfo.identifier,
            "time": pytime.ctime(SIMULATION_START),
        }
    ]
    return results


simulation_id.name = "simulation_id"
simulation_id.whitelist = (SimulationInfo,)
simulation_id.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1e-9
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1e-9
    ),
}


def hitrate_evaluation(hitrateinfo: HitrateInfo) -> list:
    results = [
        {
            "hitrate": hitrateinfo.hitrate,
            "volume": hitrateinfo.volume / 1000.0 / 1000.0 / 1000.0,
            "providesfile": hitrateinfo.provides_file,
        }
    ]
    return results


hitrate_evaluation.name = "hitrate_evaluation"
hitrate_evaluation.whitelist = (HitrateInfo,)
hitrate_evaluation.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1e-9
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis"}, resolution=1e-9
    ),
}


def storage_status(storage: StorageElement) -> list:
    """
    Log information about current storage object state
    :param storage:
    :return: list of records for logging
    """
    results = [
        {
            "storage": repr(storage),
            "usedstorage": storage.used,
            "storagesize": storage.size,
            "numberoffiles": len(storage.files),
        }
    ]
    return results


storage_status.name = "storage_status"
storage_status.whitelist = (StorageElement,)
storage_status.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "storage"}, resolution=1e-9
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "storage"}, resolution=1e-9
    ),
}


def pipe_status(pipeinfo: MonitoredPipeInfo) -> list:
    """
    Log information about the pipes
    :param storage:
    :return:
    """
    results = [
        {
            "pipe": repr(pipeinfo.pipename),
            "throughput": pipeinfo.available_throughput / 1000.0 / 1000.0 / 1000.0,
            "requested_throughput": pipeinfo.requested_throughput
            / 1000.0
            / 1000.0
            / 1000.0,
            "throughput_scale": pipeinfo.throughputscale,
            "no_subscribers": pipeinfo.no_subscriptions,
        }
    ]
    return results


pipe_status.name = "pipe_status"
pipe_status.whitelist = (MonitoredPipeInfo,)
pipe_status.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1e-9
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1e-9
    ),
}


def pipe_data_volume(pipe: MonitoredPipe):
    """
    Total amount of data transferred by the pipe up to this point
    :param pipe:
    :return:
    """
    results = [
        {
            "pipe": repr(pipe),
            "current_total": pipe.transferred_data / 1000.0 / 1000.0 / 1000.0,
        }
    ]
    return results


pipe_data_volume.name = "pipe_data_volume"
pipe_data_volume.whitelist = (MonitoredPipe,)
pipe_data_volume.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    # logging.StreamHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1e-9
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pipe"}, resolution=1e-9
    ),
}


def extended_job_events(job: CachingJob) -> List[Dict]:
    """
    Log relevant events for jobs. Relevant events are

    * start of a job,
    * finishing of a job, either successful or not.

    Information about the start of a job are relevant to enable timely analysis
    of waiting times. For finishing of jobs information about the success itself,
    but also additional information on exceeded resources or refusal by the
    drone are added.

    .. Warning::

        The logging format includes the name / identifier of a job. This might
        result in a huge index of the grafana database. The job is currently
        included to enable better lookup and analysis of related events.

    :param job: the job to log information for
    :return: list of records for logging
    """
    result = {
        "pool_configuration": "None",
        "pool_type": "drone",
        "pool": repr(job.drone),
        "job": repr(job),
        "cached": str(job._cached_data),
    }
    if job.successful is None:
        result["queue_time"] = job.queue_date
        result["waiting_time"] = job.waiting_time
        result["starting"] = 1
    elif job.successful:
        result["wall_time"] = job.walltime
        result["original_walltime"] = job._original_walltime
        result["calculation_time"] = job._calculation_time
        result["transfer_time"] = job._transfer_time
        result["success"] = 1
        result["diff"] = job.walltime - job._original_walltime
        result["efficiency"] = job.cputime * 1.0 / job.walltime
        result["read_from_cache"] = job._read_from_cache
        result["data_througput"] = (
            job._total_input_data / 1000.0 / 1000.0 / 1000.0 / job.walltime
        )
        result["cache_probability"] = job.cache_probability
        result["expectation_cached_data"] = job.expectation_cached_data
    else:
        result["success"] = 0
        error_logged = False
        for resource_key in job.resources:
            print(resource_key)
            usage = job.used_resources.get(
                resource_key, job.resources.get(resource_key, None)
            )
            print(usage, job.resources)
            print(job.drone)
            value = usage / job.resources.get(
                resource_key, job.drone.pool_resources[resource_key]
            )
            if value > 1:
                result[f"exceeded_{resource_key}"] = value
                error_logged = True
        if not error_logged:
            result["refused_by"] = repr(job.drone)
    return [result]


extended_job_events.name = "job_event"
extended_job_events.whitelist = (CachingJob,)
extended_job_events.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool", "job", "cached"},
        resolution=1e-9,
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_configuration", "pool_type", "pool", "job", "cached"},
        resolution=1e-9,
    ),
}


def drone_statistics_caching(drone: "Drone") -> List[Dict]:
    """


    :param drone: the drone
    :return: list of records for logging
    """
    full_resources = drone.pool_resources
    resources = drone.theoretical_available_resources

    results = [
        {
            "pool_type": "drone",
            "pool": repr(drone),
            "claimed_slots": full_resources["cores"] - resources["cores"],
            "free_slots": resources["cores"],
            "slots_with_caching": drone.jobs_with_cached_data,
        }
    ]
    return results


drone_statistics_caching.name = "drone_status_caching"
drone_statistics_caching.whitelist = (Drone,)
drone_statistics_caching.logging_formatter = {
    LoggingSocketHandler.__name__: JsonFormatter(),
    logging.StreamHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_type", "pool"}, resolution=1e-9
    ),
    LoggingUDPSocketHandler.__name__: LineProtocolFormatter(
        tags={"tardis", "pool_type", "pool"}, resolution=1e-9
    ),
}
