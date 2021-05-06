from typing import Optional, Callable, Tuple

from lapis.caching.files import RequestedFile, StoredFile
from lapis.caching.storageelement import StorageElement
from lapis.utilities.cache_cleanup_implementations import sort_files_by_cachedsince


def check_size(file: RequestedFile, storage: StorageElement):
    return storage.size >= file.filesize


def check_relevance(file: RequestedFile, storage: StorageElement):
    return True


def delete_oldest(
    file: RequestedFile, storage: StorageElement
) -> Tuple[bool, Optional[Tuple[StoredFile, ...]]]:
    currently_free = storage.available
    if currently_free >= file.filesize:
        return True, None
    deletable_files = []
    sorted_files = sort_files_by_cachedsince(storage.files.values())
    for current_file in sorted_files:
        deletable_files.append(current_file)
        currently_free += current_file.filesize
        if currently_free >= file.filesize:
            return True, tuple(deletable_files)
    return False, None


def delete_oldest_few_used(
    file: RequestedFile, storage: StorageElement
) -> Tuple[bool, Optional[Tuple[StoredFile]]]:
    deletable_files = []
    currently_free = storage.available
    if currently_free < storage.available:
        sorted_files = sort_files_by_cachedsince(storage.files.items())
        for current_file in sorted_files:
            if current_file.numberofaccesses < 3:
                deletable_files.append(current_file)
                currently_free += deletable_files[-1].filesize
                if currently_free >= file.filesize:
                    return True, tuple(deletable_files)
    return False, None


class CacheAlgorithm(object):
    def __init__(self, caching_strategy: Callable, deletion_strategy: Callable):
        self._caching_strategy = lambda file, storage: check_size(
            file, storage
        ) and check_relevance(file, storage)
        self._deletion_strategy = lambda file, storage: delete_oldest(file, storage)

    def consider(
        self, file: RequestedFile, storage: StorageElement
    ) -> Tuple[bool, Optional[Tuple[StoredFile]]]:
        if self._caching_strategy(file, storage):
            return self._deletion_strategy(file, storage)
        return False, None
