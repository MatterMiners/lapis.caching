from typing import Optional


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
        filesize: int = 0,
        storedsize: int = 0,
        cachedsince: Optional[float] = None,
        lastaccessed: Optional[float] = None,
        numberofaccesses: int = 0,
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

    def access(self, access_time: float):
        """
        Tracks a new access to the file at time `access_time`, including
        incrementing the access count.

        :param access_time: time when the file was accessed
        """
        self.lastaccessed = access_time
        self.numberofaccesses += 1


class RequestedFile(object):
    """
    Representation of a requested file
    """

    __slots__ = ("filename", "filesize")

    def __init__(self, filename: str, filesize: int) -> None:
        """name of the file"""
        self.filename = filename
        """size of the file"""
        self.filesize = filesize

    def to_stored_file(self, currenttime: float) -> StoredFile:
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


class RequestedFile_HitrateBased(RequestedFile):
    """
    Represents a requested file in hitrate based caching.
    The cachehitrate flag is somewhat messed up currently.
    **Its use should be reworked when remodeling the connection module.**
    """

    __slots__ = "cachehitrate"

    def __init__(self, filename: str, filesize: int, cachehitrate: int):
        super().__init__(filename, filesize)
        """flag whether the file is cached, 1 if it is cached, 0 if it is not cached"""
        self.cachehitrate = cachehitrate
