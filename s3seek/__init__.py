import io


class S3File(io.RawIOBase):
    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0
        self._size = self.s3_object.content_length

    def __repr__(self):
        return f"S3File({self.s3_object})"

    @property
    def size(self):
        # TODO check if this is cached or always requested
        return self._size

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            if offset < 0:
                raise OSError("Unable to seek before start")
            # seeking past the end is permitted
            self.position = offset
        elif whence == io.SEEK_CUR:
            if self.position + offset < 0:
                raise OSError("Unable to seek before start")
            # seeking past the end is permitted
            self.position += offset
        elif whence == io.SEEK_END:
            if offset < -self.size:
                raise OSError("Unable to seek before start")
            self.position = self.size + offset
        else:
            raise ValueError(
                "invalid whence (%r, should be %d, %d, %d)"
                % (whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
            )

        return self.position

    def readable(self):
        return True

    def read(self, size=-1):
        if size == -1 and self.position == 0:
            # special case, skip range header if reading all
            self.seek(0, whence=io.SEEK_END)
            return self.s3_object.get()["Body"].read()
        elif self.position >= self.size:
            # reading past the end of the file does little
            return b""
        elif size == -1:
            # Read to the end of the file
            range_header = f"bytes={self.position}-{self.size}"
            value = self.s3_object.get(Range=range_header)["Body"].read()
            self.seek(len(value), whence=io.SEEK_CUR)
            return value
        else:
            new_position = self.position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            range_header = f"bytes={self.position}-{new_position - 1}"
            value = self.s3_object.get(Range=range_header)["Body"].read()
            self.seek(len(value), whence=io.SEEK_CUR)
            return value

    def writable(self):
        return False

    def truncate(self):
        raise OSError("not writable")

    def write(self, bytes_to_write):
        raise OSError("not writable")


class S3FileBuffered(io.BufferedIOBase):
    def __init__(self, s3_object, buffer_max=1024 * 1024):
        self.s3_object = s3_object
        self.position = 0
        self.buffer_max = buffer_max  # default 1MB = 1048576 bytes
        self.buffer = b""
        self.raw = S3File(s3_object)
        self.count_buffer_hits = 0
        self.count_buffer_misses = 0

    def __repr__(self):
        return f"S3FileBuffered({self.s3_object})"

    @property
    def size(self):
        return self.raw.size

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        old_position = self.position

        if whence == io.SEEK_SET:
            if offset < 0:
                raise OSError("Unable to seek before start")
            # seeking past the end is permitted
            self.position = offset
        elif whence == io.SEEK_CUR:
            if self.position + offset < 0:
                raise OSError("Unable to seek before start")
            # seeking past the end is permitted
            self.position += offset
        elif whence == io.SEEK_END:
            if offset < -self.size:
                raise OSError("Unable to seek before start")
            self.position = self.size + offset
        else:
            raise ValueError(
                "invalid whence (%r, should be %d, %d, %d)"
                % (whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
            )

        # need to update the buffer as we moved
        # if its a small jump forward, jump the buffer
        if self.position >= old_position and self.position - old_position < len(
            self.buffer
        ):
            self.buffer = self.buffer[self.position - old_position :]
        else:
            # too big a change, empty the buffer
            self.buffer = b""

        return self.position

    def readable(self):
        return True

    def read(self, size=-1):
        if size >= 0:
            return self.read1(size)
        else:
            return self.read1(self.size - self.position)

    def read1(self, size):
        if size <= len(self.buffer):
            # request fits in current buffer
            # no need to get more data
            self.count_buffer_hits += 1
            value = self.buffer[:size]
            self.seek(len(value), io.SEEK_CUR)
            return value
        else:
            # need to grow buffer
            self.count_buffer_misses += 1
            # calculate how much more we need
            size_to_get = size - len(self.buffer) + self.buffer_max
            # move to the appropriate place in the raw stream
            self.raw.seek(self.position + len(self.buffer), io.SEEK_SET)
            # get the extra data
            self.buffer = self.buffer + self.raw.read(size_to_get)
            # get the piece to return
            value = self.buffer[:size]
            # move the position and trim the buffer
            self.seek(len(value), io.SEEK_CUR)
            # return the value we put together earlier
            return value

    def writable(self):
        return False

    def truncate(self):
        raise OSError("not writable")

    def write(self, bytes_to_write):
        raise OSError("not writable")
