import io

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

    def test_read_all(self, s3_obj):
        content = s3_obj.get()["Body"].read()
        s3file = S3File(s3_obj)

        # read the whole thing
        assert content == s3file.read()
        assert len(content) == s3file.size

    def test_seek(self, s3_obj):
        content = s3_obj.get()["Body"].read()
        s3file = S3File(s3_obj)

        # read the first X bytes
        content = s3_obj.get()["Body"].read()
        assert content[:16] == s3file.read(16)
        assert 16 == s3file.tell()

        # seek back to the start
        s3file.seek(0)
        assert content[:16] == s3file.read(16)
        s3file.seek(0, io.SEEK_SET)
        assert content[:16] == s3file.read(16)

        # relative seek forward
        s3file.seek(16, io.SEEK_CUR)
        assert content[32:48] == s3file.read(16)

        # relative seek backward
        s3file.seek(-16, io.SEEK_CUR)
        assert content[32:48] == s3file.read(16)
        # relative seek end
        s3file.seek(-16, io.SEEK_END)
        assert content[-16:] == s3file.read(16)

        # seek before file
        with pytest.raises(OSError):
            s3file.seek(-len(content) - 1, io.SEEK_CUR)
        with pytest.raises(OSError):
            s3file.seek(-1, io.SEEK_SET)
        with pytest.raises(OSError):
            s3file.seek(-len(content) - 1, io.SEEK_END)

        # seek after file returns b''
        s3file.seek(16, io.SEEK_END)
        assert b"" == s3file.read(16)


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

    def test_read_all(self, s3_obj):
        content = s3_obj.get()["Body"].read()
        s3file = S3FileBuffered(s3_obj, 64)

        # read the whole thing
        assert content == s3file.read()
        assert len(content) == s3file.size

    def test_seek(self, s3_obj):
        content = s3_obj.get()["Body"].read()
        s3file = S3FileBuffered(s3_obj, 64)

        # first 80 bytes are:
        # b'2015-09-04\n\nModification to: misc,bas\n\nDetails can be found in\nchangelog_details'
        #   0                 16              32                 48                64

        # read the first X bytes
        content = s3_obj.get()["Body"].read()
        assert content[:16] == s3file.read(16)
        assert 16 == s3file.tell()

        # seek back to the start
        s3file.seek(0)
        assert content[:16] == s3file.read(16)
        s3file.seek(0, io.SEEK_SET)
        assert content[:16] == s3file.read(16)

        # relative seek forward
        s3file.seek(16, io.SEEK_CUR)
        assert content[32:48] == s3file.read(16)

        # relative seek backward
        s3file.seek(-16, io.SEEK_CUR)
        assert content[32:48] == s3file.read(16)
        # relative seek end
        s3file.seek(-16, io.SEEK_END)
        assert content[-16:] == s3file.read(16)

        # read over buffer end
        s3file.seek(0)
        assert content[:16] == s3file.read(16)
        assert content[16 : 16 + 128] == s3file.read(128)

        # seek before file
        with pytest.raises(OSError):
            s3file.seek(-len(content) - 1, io.SEEK_CUR)
        with pytest.raises(OSError):
            s3file.seek(-1, io.SEEK_SET)
        with pytest.raises(OSError):
            s3file.seek(-len(content) - 1, io.SEEK_END)

        # seek after file returns b''
        s3file.seek(16, io.SEEK_END)
        assert b"" == s3file.read(16)
