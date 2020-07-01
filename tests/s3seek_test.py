import io
import os.path

import pytest

from s3seek import S3File, S3FileBuffered


@pytest.fixture
def s3_obj():
    # this is a public file we can use
    # not ideal, but simpler than mocking for now
    import boto3

    resource = boto3.resource("s3")
    from botocore.handlers import disable_signing

    resource.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
    obj = resource.Object(bucket_name="1000genomes", key="CHANGELOG")
    return obj


@pytest.fixture
def s3_local():
    # this is a public file we can use
    # not ideal, but simpler than mocking for now
    pth = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data", "1000genomes_CHANGELOG"
    )
    return open(pth, "rb")


class TestS3File:
    def test_info(self, s3_obj):
        s3file = S3File(s3_obj)
        assert s3file.seekable()
        assert s3file.readable()

    def test_not_writable(self, s3_obj):
        s3file = S3File(s3_obj)
        assert not s3file.writable()
        with pytest.raises(OSError):
            assert s3file.write(b"")
        with pytest.raises(OSError):
            assert s3file.truncate()

    def test_read_all(self, s3_obj, s3_local):
        content = s3_local.read()
        s3file = S3File(s3_obj)

        # read the whole thing
        assert 0 == s3file.tell()
        assert content == s3file.read()
        assert len(content) == s3file.size
        assert len(content) == s3file.tell()

    def test_seek(self, s3_obj, s3_local):
        content = s3_local.read()
        s3file = S3File(s3_obj)

        # read the first X bytes
        assert 0 == s3file.tell()
        assert content[:16] == s3file.read(16)
        assert 16 == s3file.tell()

        # seek back to the start
        s3file.seek(0)
        assert 0 == s3file.tell()
        assert content[:16] == s3file.read(16)
        assert 16 == s3file.tell()

        s3file.seek(0, io.SEEK_SET)
        assert 0 == s3file.tell()
        assert content[:16] == s3file.read(16)
        assert 16 == s3file.tell()

        # relative seek forward
        s3file.seek(16, io.SEEK_CUR)
        assert 32 == s3file.tell()
        assert content[32:48] == s3file.read(16)
        assert 48 == s3file.tell()

        # relative seek backward
        s3file.seek(-16, io.SEEK_CUR)
        assert 32 == s3file.tell()
        assert content[32:48] == s3file.read(16)
        assert 48 == s3file.tell()

        # relative seek end
        s3file.seek(-16, io.SEEK_END)
        assert len(content) - 16 == s3file.tell()
        assert content[-16:] == s3file.read(16)
        assert len(content) == s3file.tell()

        # seek before file
        with pytest.raises(OSError):
            s3file.seek(-len(content) - 1, io.SEEK_CUR)
        with pytest.raises(OSError):
            s3file.seek(-1, io.SEEK_SET)
        with pytest.raises(OSError):
            s3file.seek(-len(content) - 1, io.SEEK_END)

        # seek after file returns b''
        s3file.seek(16, io.SEEK_END)
        assert len(content) + 16 == s3file.tell()
        assert b"" == s3file.read(16)
        # read after end of file doesn't move tell because no bytes returned
        assert len(content) + 16 == s3file.tell()


class TestS3FileBuffered:
    def test_info(self, s3_obj):
        s3file = S3FileBuffered(s3_obj, 64)
        assert s3file.seekable()
        assert s3file.readable()

    def test_not_writable(self, s3_obj):
        s3file = S3FileBuffered(s3_obj, 64)
        assert not s3file.writable()
        with pytest.raises(OSError):
            assert s3file.write(b"")
        with pytest.raises(OSError):
            assert s3file.truncate()

    def test_read_all(self, s3_obj, s3_local):
        content = s3_local.read()
        s3file = S3FileBuffered(s3_obj, 64)

        # read the whole thing
        assert content == s3file.read()
        assert len(content) == s3file.size

    def test_seek(self, s3_obj, s3_local):
        content = s3_local.read()
        s3file = S3FileBuffered(s3_obj, 64)

        # first 80 bytes are:
        # b'2015-09-04\n\nModification to: misc,bas\n\nDetails can be found in\nchangelog_details'
        #   0                 16              32                 48                64

        # read the first X bytes
        assert 0 == s3file.tell()
        assert content[:16] == s3file.read(16)
        assert 16 == s3file.tell()

        # seek back to the start
        s3file.seek(0)
        assert 0 == s3file.tell()
        assert content[:16] == s3file.read(16)
        assert 16 == s3file.tell()

        s3file.seek(0, io.SEEK_SET)
        assert 0 == s3file.tell()
        assert content[:16] == s3file.read(16)
        assert 16 == s3file.tell()

        # relative seek forward
        s3file.seek(16, io.SEEK_CUR)
        assert 32 == s3file.tell()
        assert content[32:48] == s3file.read(16)
        assert 48 == s3file.tell()

        # relative seek backward
        s3file.seek(-16, io.SEEK_CUR)
        assert 32 == s3file.tell()
        assert content[32:48] == s3file.read(16)
        assert 48 == s3file.tell()

        # relative seek end
        s3file.seek(-16, io.SEEK_END)
        assert len(content) - 16 == s3file.tell()
        assert content[-16:] == s3file.read(16)
        assert len(content) == s3file.tell()

        # seek before file
        with pytest.raises(OSError):
            s3file.seek(-len(content) - 1, io.SEEK_CUR)
        with pytest.raises(OSError):
            s3file.seek(-1, io.SEEK_SET)
        with pytest.raises(OSError):
            s3file.seek(-len(content) - 1, io.SEEK_END)

        # seek after file returns b''
        s3file.seek(16, io.SEEK_END)
        assert len(content) + 16 == s3file.tell()
        assert b"" == s3file.read(16)
        # read after end of file doesn't move tell because no bytes returned
        assert len(content) + 16 == s3file.tell()

    def test_seek_buffer(self, s3_obj, s3_local):
        content = s3_local.read()
        s3file = S3FileBuffered(s3_obj, 64)

        # first 80 bytes are:
        # b'2015-09-04\n\nModification to: misc,bas\n\nDetails can be found in\nchangelog_details'
        #   0                 16              32                 48                64

        # read ahead in buffer
        s3file.seek(0)
        assert content[:16] == s3file.read(16)
        assert content[16:32] == s3file.read(16)
        s3file.seek(16, io.SEEK_CUR)
        assert content[48:64] == s3file.read(16)

        # read over buffer end
        s3file.seek(0)
        assert content[:16] == s3file.read(16)
        assert content[16 : 16 + 128] == s3file.read(128)
        s3file.seek(128)
        assert content[128 : 16 + 128] == s3file.read(16)
