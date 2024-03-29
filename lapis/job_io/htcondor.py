import csv
import json
import logging
from typing import Optional

from lapis.cachingjob import CachingJob
from copy import deepcopy


def htcondor_job_reader(
    iterable,
    calculation_efficiency: Optional[float] = None,
    resource_name_mapping={  # noqa: B006
        "cores": "RequestCpus",
        "walltime": "RequestWalltime",  # s
        "memory": "RequestMemory",  # MiB
        "disk": "RequestDisk",  # KiB
    },
    used_resource_name_mapping={  # noqa: B006
        "queuetime": "QDate",
        "walltime": "RemoteWallClockTime",  # s
        "memory": "MemoryUsage",  # MB
        "disk": "DiskUsage_RAW",  # KiB
    },
    unit_conversion_mapping={  # noqa: B006
        "RequestCpus": 1,
        "RequestWalltime": 1,
        "RequestMemory": 1024 * 1024,
        "RequestDisk": 1024,  # KBytes
        "queuetime": 1,
        "RemoteWallClockTime": 1,
        "MemoryUsage": 1000 * 1000,  # MB
        "DiskUsage_RAW": 1024,  # KBytes
        "filesize": 1000 * 1000 * 1000,  # GB
        "usedsize": 1000 * 1000 * 1000,  # GB
    },
):
    input_file_type = iterable.name.split(".")[-1].lower()
    if input_file_type == "json":
        htcondor_reader = json.load(iterable)
    elif input_file_type == "csv":
        htcondor_reader = csv.DictReader(iterable, delimiter=" ", quotechar="'")
    else:
        logging.getLogger("implementation").error(
            "Invalid input file %s. CachingJob input file can not be read."
            % iterable.name
        )
    for entry in htcondor_reader:
        if float(entry[used_resource_name_mapping["walltime"]]) <= 0:
            logging.getLogger("implementation").warning(
                "removed job from htcondor import (%s)", entry
            )
            continue
        resources = {}
        for key, original_key in resource_name_mapping.items():
            try:
                resources[key] = int(
                    float(entry[original_key])
                    * unit_conversion_mapping.get(original_key, 1)
                )
            except ValueError:
                pass

        used_resources = {
            "cores": (
                (float(entry["RemoteSysCpu"]) + float(entry["RemoteUserCpu"]))
                / float(entry[used_resource_name_mapping["walltime"]])
            )
            * unit_conversion_mapping.get(resource_name_mapping["cores"], 1)
        }
        for key in ["memory", "walltime", "disk"]:
            original_key = used_resource_name_mapping[key]
            used_resources[key] = int(
                float(entry[original_key])
                * unit_conversion_mapping.get(original_key, 1)
            )

        calculation_efficiency = entry.get(
            "calculation_efficiency", calculation_efficiency
        )

        try:
            if not entry["Inputfiles"]:
                del entry["Inputfiles"]
                raise KeyError
            resources["inputfiles"] = deepcopy(entry["Inputfiles"])
            used_resources["inputfiles"] = deepcopy(entry["Inputfiles"])
            for filename, filespecs in entry["Inputfiles"].items():
                for key in filespecs.keys():
                    if key == "hitrates":
                        continue
                    resources["inputfiles"][filename][key] = filespecs[
                        key
                    ] * unit_conversion_mapping.get(key, 1)
                    used_resources["inputfiles"][filename][key] = filespecs[
                        key
                    ] * unit_conversion_mapping.get(key, 1)

                if "usedsize" in filespecs:
                    del resources["inputfiles"][filename]["usedsize"]

                if "filesize" in filespecs:
                    if "usedsize" not in filespecs:
                        used_resources["inputfiles"][filename]["usedsize"] = resources[
                            "inputfiles"
                        ][filename]["filesize"]
                    del used_resources["inputfiles"][filename]["filesize"]

        except KeyError:
            pass
        yield CachingJob(
            resources=resources,
            used_resources=used_resources,
            queue_date=float(entry[used_resource_name_mapping["queuetime"]]),
            calculation_efficiency=calculation_efficiency,
            name=entry.get("name", None),
        )
