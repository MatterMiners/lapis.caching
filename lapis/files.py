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
        Intialization of a stored file

        :param filename: name of the file
        :param filesize: size of the file
        :param storedsize: size of the file that is actually stored, necessary if
        less than the whole file is stored
        :param cachedsince: time when the file was cached
        :param lastaccessed: time when the file was accessed the last time
        :param numberofaccesses: number of times the file was accessed
        """
        self.filename = filename
        """name of the file """
        self.filesize = filesize
        """size of the file"""
        self.storedsize = storedsize or self.filesize
        """size of the file that is actually stored"""
        self.cachedsince = cachedsince
        """point in time when the file was cached"""
        self.lastaccessed = lastaccessed
        """time when the file was accessed the last time"""
        self.numberofaccesses = numberofaccesses
        """number of times the file was accessed"""

    def increment_accesses(self):
        """
        Increments number of accesses of a file
        """
        self.numberofaccesses += 1


class RequestedFile(NamedTuple):
    """
    Representation of a requested file
    """
    filename: str
    """name of the file"""
    filesize: Optional[int] = None
    """size of the file"""

    def convert_to_stored_file_object(self, currenttime):
        """
        Converts a requested file into a stored file

        :param currenttime: point in time when the conversion takes place
        """
        print(self.filesize)
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
