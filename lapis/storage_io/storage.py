import csv
from functools import partial

from lapis.caching.files import StoredFile


def storage_reader(
    storage,
    storage_content,
    storage_type,
    unit_conversion_mapping: dict = {  # noqa: B006
        "cachesizeGB": 1000 * 1000 * 1000,  # GB
        "throughput_limit": 1000 * 1000 * 1000,  # GB
    },
):
    try:
        storage_content = storage_content_reader(storage_content)
    except TypeError:
        storage_content = dict()
    reader = csv.DictReader(storage, delimiter=" ", quotechar="'")
    for row in reader:
        yield partial(
            storage_type,
            name=row["name"],
            sitename=row["sitename"],
            size=int(
                float(row["cachesizeGB"])
                * unit_conversion_mapping.get("cachesizeGB", 1)
            ),
            throughput_limit=int(
                float(row["throughput_limit"])
                * unit_conversion_mapping.get("throughput_limit", 1)
            ),
            files=storage_content.get(row["name"], dict()),
        )()


def storage_content_reader(
    file_name,
    unit_conversion_mapping: dict = {  # noqa: B006
        "filesize": 1000 * 1000 * 1000,
        "storedsize": 1000 * 1000 * 1000,
    },
):
    reader = csv.DictReader(file_name, delimiter=" ", quotechar="'")
    cache_information = dict()
    for row in reader:
        for key in row:
            if key not in ["filename", "cachename"]:
                row[key] = int(float(row[key]) * unit_conversion_mapping.get(key, 1))
        cache_information.setdefault(row["cachename"], {})[
            row["filename"]
        ] = StoredFile(**row)
    return cache_information


def storage_reader_filebased_hitrate_caching(
    storage,
    storage_type,
    storage_content=None,
    unit_conversion_mapping: dict = {  # noqa: B006
        "cachesizeGB": 1000 * 1000 * 1000,
        "throughput_limit": 1000 * 1000 * 1000,
    },
):

    reader = csv.DictReader(storage, delimiter=" ", quotechar="'")
    for row in reader:
        yield partial(
            storage_type,
            name=row["name"],
            sitename=row["sitename"],
            size=int(
                float(row["cachesizeGB"])
                * unit_conversion_mapping.get("cachesizeGB", 1)
            ),
            throughput_limit=int(
                float(row["throughput_limit"])
                * unit_conversion_mapping.get("throughput_limit", 1)
            ),
            files=dict(),
        )()
