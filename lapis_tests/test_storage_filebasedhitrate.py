from lapis.caching.storageelement import FileBasedHitrateStorage
from lapis_tests import via_usim
from lapis.caching.files import RequestedFile_HitrateBased
from lapis.caching.storageelement import LookUpInformation

from usim import time

import pytest


class TestFileBasedHitrateStorag:
    def test_storage_initialization(self):
        filebasedhitratestorage = FileBasedHitrateStorage(
            files={}, name="name", sitename="site", size=200, throughput_limit=1
        )
        assert filebasedhitratestorage.files == {}
        assert filebasedhitratestorage.name == "name"
        assert filebasedhitratestorage.sitename == "site"
        assert filebasedhitratestorage.size == 200
        assert filebasedhitratestorage.connection.throughput == 1

        assert filebasedhitratestorage.available == 200
        assert filebasedhitratestorage.used == 0

    @via_usim
    async def test_transfer(self):
        filebasedhitratestorage = FileBasedHitrateStorage(
            files={}, name="name", sitename="site", size=200, throughput_limit=1
        )
        requestedFile = RequestedFile_HitrateBased("filename", 20, 1)
        await filebasedhitratestorage.transfer(requestedFile)
        assert time.now == 20

        with pytest.raises(ValueError):
            requestedFile = RequestedFile_HitrateBased("filename", 20, 0)
            await filebasedhitratestorage.transfer(requestedFile)

    def test_find_file_in_storage(self):
        filebasedhitratestorage = FileBasedHitrateStorage(
            files={}, name="name", sitename="site", size=200, throughput_limit=1
        )
        requestedFile = RequestedFile_HitrateBased("filename", 20, 1)
        foundFile = LookUpInformation(20, filebasedhitratestorage)

        assert filebasedhitratestorage.find(requestedFile) == foundFile

    @via_usim
    async def test_modification_of_stored_files(self):
        filebasedhitratestorage = FileBasedHitrateStorage(
            files={}, name="name", sitename="site", size=200, throughput_limit=1
        )
        requestedFile = RequestedFile_HitrateBased("filename", 20, 1)

        await filebasedhitratestorage.add(requestedFile)
        assert filebasedhitratestorage.files == {}

        stored_file = requestedFile.to_stored_file(time.now)

        await filebasedhitratestorage.remove(stored_file)
        assert filebasedhitratestorage.files == {}
