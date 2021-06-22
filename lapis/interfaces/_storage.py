import abc

from typing import NamedTuple

from lapis.caching.files import RequestedFile, StoredFile


class TransferStatistics(NamedTuple):
    bytes_from_remote: int
    """bytes transferred from remote"""
    bytes_from_cache: int
    """bytes transferred from cache"""


class LookUpInformation(NamedTuple):
    cached_filesize: int
    storage: "Storage"


class Storage(metaclass=abc.ABCMeta):
    """
    This class represents the basic structures of all representations of storage
    in this simulation.
    """

    @property
    @abc.abstractmethod
    def size(self) -> int:
        """Total size of storage in Bytes"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def available(self) -> int:
        """Available storage in Bytes"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def used(self) -> int:
        """Used storage in Bytes"""
        raise NotImplementedError

    @abc.abstractmethod
    async def transfer(self, file: RequestedFile) -> TransferStatistics:
        """
        Transfer size of given file via the storages' connection and update file
        information. If the file was deleted since it was originally looked up
        the resulting error is not raised.

        .. TODO:: What does this mean with the error?
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def add(self, file: RequestedFile):
        """
        Add file information to storage and transfer the size of the file via
        the storages' connection.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def remove(self, file: StoredFile):
        """
        Remove all file information and used file size from the storage.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def find(self, file: RequestedFile) -> LookUpInformation:
        """Information if a file is stored in Storage"""
        raise NotImplementedError
