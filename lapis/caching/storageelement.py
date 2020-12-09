from typing import Optional

from usim import time, Resources, Scope
from lapis.caching.monitoredpipe import MonitoredPipe
from lapis.monitor import sampling_required

from lapis.caching.files import StoredFile, RequestedFile, RequestedFile_HitrateBased
from lapis.interfaces._storage import Storage, LookUpInformation

import logging


class RemoteStorage(Storage):
    """
    The RemoteStorage object represents the entirety of (WLCG) grid storage. All
    files that can be requested by a job are provided by remote storage and it's size
    is therefore approximated as infinite. Files are transferred from this storage
    via the associated pipe, a network bandwidth model. There can be multiple remote
    storages in the simulation because resource pools may have differing network
    connections.
    """
    # TODO:: ensure that there can be multiple remote storages in the simulation
    def __init__(self, throughput: float):
        """
        Initialization of the remote storages pipe, representing the network
        connection to remote storage with a limited bandwidth.

        :param pipe:
        """
        conversion_GB_to_B = 1000 * 1000 * 1000
        self.connection = MonitoredPipe(throughput=throughput * conversion_GB_to_B)
        self.connection.storage = repr(self)

    @property
    def size(self):
        return float("Inf")

    @property
    def available(self):
        return float("Inf")

    @property
    def used(self):
        return 0

    async def transfer(self, file: RequestedFile, **kwargs):
        """
        Simulates the transfer of a requested file via the remote storage's pipe.

        :param file: representation of the requested file
        """
        await self.connection.transfer(total=file.filesize)
        await sampling_required.put(self.connection)

    async def add(self, file: StoredFile, **kwargs):
        """
        All files are contained in remote storage. Therefore no functionality to
        adding files is provided.
        """
        raise NotImplementedError

    async def remove(self, file: StoredFile, **kwargs):
        """
        All files are contained in remote storage. Therefore no functionality
        to removing files is provided.
        """
        raise NotImplementedError

    def find(self, file: RequestedFile, **kwargs) -> LookUpInformation:
        """
        All files are contained in remote storage. Therefore no functionality
        to determine whether the storage contains a certain file is provided.
        """
        raise NotImplementedError


class StorageElement(Storage):
    """
    The StorageElement object represents a local data storage or cache containing an
    exact list of files and providing functionality to transfer and change the
    storage's content.
    """

    __slots__ = (
        "name",
        "sitename",
        "_size",
        "deletion_duration",
        "update_duration",
        "_usedstorage",
        "files",
        "filenames",
        "connection",
        "remote_storage",
    )

    def __init__(
        self,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        size: int = 1000 * 1000 * 1000 * 1000,
        throughput_limit: int = 10 * 1000 * 1000 * 1000,
        files: Optional[dict] = None,
        deletion_duration: float = 5,
        update_duration: float = 1
    ):
        """
        Intialization of a storage element object.

        :param name: identification of the storage
        :param sitename: identifier, drones with the same sitename can access this
        storage
        :param size: total size of the storage in bytes
        :param throughput_limit: maximal bandwidth of the network connection to this
        storage
        :param files: dictionary of the files that are currently stored
        :param deletion_duration: in seconds, amount of time passing while a file is
        deleted from the storage
        :param update_duration:  in seconds, amount of time passing while a file's
        information is updated
        """
        self.name = name
        """identification of the storage"""
        self.sitename = sitename
        """identifier, drones with the same sitename can access this 
        storage"""
        self.deletion_duration = deletion_duration
        """amount of time passing while a file is deleted from the storage"""
        self.update_duration = update_duration
        """amount of time passing while a file's information is updated"""
        self._size = size
        """size of the storage"""
        self.files = files
        """dict of files currently in the storage"""
        self._usedstorage = Resources(
            size=sum(file.storedsize for file in files.values())
        )
        """amount of storage space that is currently in use"""
        self.connection = MonitoredPipe(throughput_limit)
        """Pipe representing the network connection to this storage
        **Namespace problem between connection module and this pipe called 
        connection**"""
        self.connection.storage = repr(self)

        self.remote_storage = None
        """remote storage that provides files that are not stored in the cache"""

    @property
    def size(self):
        return self._size

    @property
    def used(self):
        return self._usedstorage.levels.size

    @property
    def available(self):
        return self.size - self.used

    async def remove(self, file: StoredFile, job_repr=None):
        """
        Deletes file from storage object. The time this operation takes is defined
        by the storages deletion_duration attribute.

        :param file: representation of the file that is removed from the storage
        :param job_repr: Needed for debug output, will be replaced
        """
        await (time + self.deletion_duration)
        await self._usedstorage.decrease(size=file.filesize)
        self.files.pop(file.filename)

    async def add(self, file: RequestedFile, job_repr=None):
        """
        Adds file to storage object transferring it through the storage object's
        connection. This should be sufficient for now because files are only added
        to the storage when they are also transferred through the Connections remote
        connection. If this simulator is extended to include any kind of
        direct file placement this has to be adapted.

        :param file: representation of the file that is added to the storage
        :param job_repr: Needed for debug output, will be replaced
        """

        file = file.convert_to_stored_file_object(time.now)
        await self._usedstorage.increase(size=file.filesize)
        self.files[file.filename] = file
        await self.connection.transfer(file.filesize)

    async def _update(self, stored_file: StoredFile, job_repr):
        """
        Updates a stored files information upon access.

        :param stored_file:
        :param job_repr: Needed for debug output, will be replaced
        :return:
        """
        await (time + self.update_duration)
        stored_file.lastaccessed = time.now
        stored_file.increment_accesses()

    async def transfer(self, file: RequestedFile, job_repr=None):
        """
        Manages file transfer via the storage elements connection and updates file
        information. If the file should have been deleted since it was originally
        looked up the resulting error is not raised.

        :param file:
        :param job_repr:  Needed for debug output, will be replaced
        """
        await self.connection.transfer(file.filesize)
        try:
            # TODO: needs handling of KeyError
            await self._update(self.files[file.filename], job_repr)
        except AttributeError:
            pass

    def find(self, requested_file: RequestedFile, job_repr=None):
        """
        Searches storage object for the requested_file and sends result (amount of
        cached data, storage object) to the queue.

        :param requested_file:
        :param job_repr: Needed for debug output, will be replaced
        :return: (amount of cached data, storage object)
        """
        try:
            result = LookUpInformation(
                self.files[requested_file.filename].filesize, self
            )
        except KeyError:
            result = LookUpInformation(0, self)
        return result

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.name or id(self))


class HitrateStorage(StorageElement):
    """
    This class was used in early simulation concepts but is outdated now!
    You're probably looking for FileBasedHitrateStorage instead!

    Simplified storage object, used to simulate a simplified form of hitrate based
    caching.  No explicit list of stored files is kept. Instead, it is assumed that a
    fraction `_hitrate` of all files is stored. Every time a file is requested from
    this kind of storage, `_hitrate` percent of the file are found on and transferred from this storage.
    1 - `_hitrate` percent of the file are transferred from the remote storage
    associated to the hitrate storage.
    """
    def __init__(
        self,
        hitrate,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        size: int = 1000 * 1000 * 1000 * 1000,
        throughput_limit: int = 10 * 1000 * 1000 * 1000,
        files: Optional[dict] = None,
    ):
        super(HitrateStorage, self).__init__(
            name=name,
            sitename=sitename,
            size=size,
            throughput_limit=throughput_limit,
            files=files,
        )
        self._hitrate = hitrate
        """global cache hitrate of this cache"""

    @property
    def available(self):
        return self.size

    @property
    def used(self):
        return 0

    async def transfer(self, file: RequestedFile, job_repr=None):
        """
        Every time a file is requested from this kind of storage, `_hitrate` percent
        of the file are found on and transferred from this storage.
        1 - `_hitrate` percent of the file are transferred from the remote storage
        associated to the hitrate storage.

        :param file:
        :param job_repr:
        """
        async with Scope() as scope:
            logging.getLogger("implementation").warning(
                "{} {} @ {} in {}".format(
                    self._hitrate * file.filesize,
                    (1 - self._hitrate) * file.filesize,
                    time.now,
                    file.filename[-30:],
                )
            )
            scope.do(self.connection.transfer(total=self._hitrate * file.filesize))
            scope.do(
                self.remote_storage.connection.transfer(
                    total=(1 - self._hitrate) * file.filesize
                )
            )

    def find(self, requested_file: RequestedFile, job_repr=None):
        return LookUpInformation(requested_file.filesize, self)

    async def add(self, file: RequestedFile, job_repr=None):
        """
        As files are not contained explicitly, no functionality to add files is
        needed
        """
        pass

    async def remove(self, file: StoredFile, job_repr=None):
        """
        As files are not contained explicitly, no functionality to remove files is
        needed
        """
        pass


class FileBasedHitrateStorage(StorageElement):
    """
    Simplified storage object. There is no explicit list of contained files.
    Instead, it is stated in file information (`RequestedFile_HitrateBased`)
    whether this file is currently stored. Whether this is the case was determined in
    the connection module's file transfer functionality.
    The definition of the storage objects size is currently irrelevant.

    #TODO: this storage object has become very intermingled with the connection module and should be tidied up and restructured!
    """
    def __init__(
        self,
        name: Optional[str] = None,
        sitename: Optional[str] = None,
        size: int = 1000 * 1000 * 1000 * 1000,
        throughput_limit: int = 10 * 1000 * 1000 * 1000,
        files: Optional[dict] = None,
    ):
        super(FileBasedHitrateStorage, self).__init__(
            name=name,
            sitename=sitename,
            size=size,
            throughput_limit=throughput_limit,
            files={},
        )

    @property
    def available(self):
        return self.size

    @property
    def used(self):
        return 0

    async def transfer(self, file: RequestedFile_HitrateBased, job_repr=None):
        if file.cachehitrate:
            await self.connection.transfer(total=file.filesize)
            await sampling_required.put(self.connection)
        else:
            print("wants to read from remote")
            print("file is not cached but cache is file source, this should not occur")
            raise ValueError

    def find(self, requested_file: RequestedFile_HitrateBased, job_repr=None):
        """
        Returns the expectation value for the amount of data of this file that are
        cached.

        :param requested_file:
        :param job_repr:
        :return: result of the lookup
        """
        return LookUpInformation(
            requested_file.filesize * requested_file.cachehitrate, self
        )

    async def add(self, file: RequestedFile, job_repr=None):
        """
        As there is no explicit record of stored files, no functionality to add files is
        needed
        """
        pass

    async def remove(self, file: StoredFile, job_repr=None):
        """
        As there is no explicit record of stored files, no functionality to
        remove files is needed
        """
        pass
