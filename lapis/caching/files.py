from typing import Optional, NamedTuple


class StoredFile(object):
    """
    Object representing stored files

    """

    __slots__ = (
        "filename",
        "filesize",
        "storedsize",
        "cachedsince",
        "lastaccessed",
        "numberofaccesses",
    )

    def __init__(
        self,
        filename: str,
        filesize: Optional[int] = None,
        storedsize: Optional[int] = None,
        cachedsince: Optional[int] = None,
        lastaccessed: Optional[int] = None,
        numberofaccesses: Optional[int] = None,
        **filespecs,
    ):
        """
        Initialization of a stored file

        :param filename: name of the file
        :param filesize: size of the file
        :param storedsize: size of the file that is actually stored, necessary if
        less than the whole file is stored
        :param cachedsince: time when the file was cached
        :param lastaccessed: time when the file was accessed the last time
        :param numberofaccesses: number of times the file was accessed
        """
        self.filename = filename
        self.filesize = filesize
        self.storedsize = storedsize or self.filesize
        self.cachedsince = cachedsince
        self.lastaccessed = lastaccessed
        self.numberofaccesses = numberofaccesses

    def access(self, access_time: int):
        """
        Tracks a new access to the file at time `access_time`, including
        incrementing the access count.

        :param access_time: time when the file was accessed
        """
        self.lastaccessed = access_time
        self.numberofaccesses += 1


class RequestedFile(NamedTuple):
    """
    Representation of a requested file
    """

    filename: str
    """name of the file"""
    filesize: Optional[int] = None
    """size of the file"""

    def to_stored_file(self, currenttime: int) -> StoredFile:
        """
        Converts a requested file into a stored file

        :param currenttime: point in time when the conversion takes place
        """
        return StoredFile(
            self.filename,
            filesize=self.filesize,
            cachedsince=currenttime,
            lastaccessed=currenttime,
            numberofaccesses=1,
        )


class RequestedFile_HitrateBased(NamedTuple):
    """
    Represents a requested file in hitrate based caching.
    The cachehitrate flag is somewhat messed up currently.
    **Its use should be reworked when remodeling the connection module.**
    """

    filename: str
    """name of the requested file"""
    filesize: int
    """size of the requested file"""
    cachehitrate: int
    """flag whether the file is cached, 1 if it is cached, 0 if it is not cached"""
