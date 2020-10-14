from usim import time

from lapis.storageelement import RemoteStorage
from lapis_tests import via_usim
from lapis.files import RequestedFile


class TestRemoteStorage(object):

    def test_throughput(self):
        remote_storage = RemoteStorage(2.0)
        assert remote_storage.connection.throughput == 2000000000

    def test_size(self):
        remote_storage = RemoteStorage(1.0)
        assert remote_storage.size == float("Inf")
        assert remote_storage.available == float("Inf")
        assert remote_storage.used == 0

    @via_usim
    async def test_transfer(self):
        remote_storage = RemoteStorage(1.0)
        requested_file = RequestedFile("testfile", 10*1000*1000*1000)
        await remote_storage.transfer(requested_file)
        assert time.now == 10



