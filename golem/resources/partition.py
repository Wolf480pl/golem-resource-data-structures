import collections
import math
import merkle
import os
import io

from golem.collections.bitarray import BitArray
from gevent.fileobject import FileObjectThread
from threading import RLock


DEFAULT_CHUNK_SIZE = 2048


class FileWrapper(object):

    @classmethod
    def open(cls, path, mode='r+b'):
        return open(path, mode)


class GeventFileWrapper(FileWrapper):

    @classmethod
    def open(cls, path, mode='r+b'):
        fd = super(GeventFileWrapper, cls).open(path, mode)

        from gevent.fileobject import FileObjectThread
        return FileObjectThread(fd, mode)


class ChunkStream(io.BufferedIOBase):
    def __init__(self, partition, chunk_num):
        self._partition = partition
        self._chunk_num = chunk_num
        self._length = partition._chunk_size
        self._off = 0

    def __read(self, size, one_call=False):
        # TODO: Respect one_call=True and call at most one read1 on the underlying files
        if size < 0:
            size = self._length
        size = min(self._length - self._off, size)
        data = self._partition.read(self._chunk_num, size, self._off)
        self._off += len(data)
        return data

    def read(self, size=-1):
        return self.__read(size)

    def read1(self, size=-1):
        return self.__read(size, one_call=True)

    def write(self, data):
        if len(data) > self._length - self._off:
            raise IndexError
        size = self._partition.write(self._chunk_num, data, self._off)
        self._off += size
        return size

    def flush(self):
        super(ChunkStream, self).flush()
        self.fh.flush()

    def seek(self, offset, whence=io.SEEK_SET):
        assert whence in [0, 1, 2]
        if whence == 0:
            self.off = offset
        elif whence == 1:
            self.off += offset
        elif whence == 2:
            self.off = max(self.length, self.length + offset)
        return self.off


class Partition(object):
    """
        Stores a list of file paths and sizes.
        Files are partitioned by chunks, and can be accesed by chunk
        number, i.e. partition[12].
    """

    file_wrapper = GeventFileWrapper

    def __init__(self, paths, chunk_size=DEFAULT_CHUNK_SIZE):

        assert isinstance(paths, collections.Sequence)
        assert len(paths) > 0
        assert chunk_size > 0

        # Read file sizes
        sizes = [os.path.getsize(p) for p in paths]

        # Structure
        self._paths = paths
        self._sizes = sizes
        self._size = sum(sizes)
        self._chunk_size = chunk_size

        # Iteration
        self._iter_offset = None
        self._iter_idx = 0
        self._iter_file = None

        self._files = [None] * len(self._sizes)
        self._locks = [RLock()] * len(self._sizes)
        self._open = False

    @staticmethod
    def allocate(paths, sizes, chunk_size=DEFAULT_CHUNK_SIZE,
                 fill=b'0', data_size=65536):
        """ Allocates disk space for files.
            Creates a new Partition instance.  """
        for path, size in zip(paths, sizes):
            # Create directories
            dir_path = os.path.dirname(path)
            if not os.path.exists(dir_path):
                os.makedirs(path, exist_ok=True)

            # Write file contents
            written = 0
            data = fill * data_size
            with open(path, 'wb') as out:
                while written < size:
                    length = min(data_size, size - written)
                    written += out.write(data[:length])

        return Partition(paths, chunk_size=chunk_size)

    def merkle(self):
        """ Creates a Merkle tree from chunks """
        tree = merkle.MerkleTree()
        was_open = self.is_open()

        if not was_open:
            self.open()

        try:
            for chunk in self:
                tree.add(chunk)
        finally:
            if not was_open:
                self.close()

        tree.build()
        return tree

    def size(self):
        """ Returns the number of chunks """
        return int(math.ceil(self._size / self._chunk_size))

    def offset(self, chunk_num, extra_offset=0):
        """ Returns the file index and offset of a chunk with given number """
        
        if not 0 <= extra_offset <= self._chunk_size:
            raise IndexError('extra_offset out of range')

        offset = chunk_num * self._chunk_size + extra_offset

        if not 0 <= offset <= self._size:
            raise IndexError('Index out of range')

        for idx, size in enumerate(self._sizes):
            if offset - size < 0:
                return idx, offset
            offset -= size

        return None, None

    def is_open(self):
        """ Returns whether partitioned files were opened """
        return self._open

    def open(self):
        """ Opens and stores handles to partitioned files """
        for idx, fd in enumerate(self._files):
            if fd:
                continue
            with self._locks[idx]:
                self._files[idx] = self.file_wrapper.open(
                    self._paths[idx])

        self._open = True

    def close(self):
        """ Closes any open file handles """
        for idx, fd in enumerate(self._files):
            with self._locks[idx]:
                fd and fd.close()

        self._open = False

    def __getitem__(self, chunk_num):
        idx, offset = self.offset(chunk_num)
        return self.__read(idx, offset )[0]

    def __setitem__(self, chunk_num, data):
        idx, offset = self.offset(chunk_num)
        return self.__write(idx, offset, data)[0]

    def get_stream(self, chunk_num):
        if not 0 <= chunk_num < self.size():
            raise IndexError('chunk_num out of range')
        return ChunkStream(self, chunk_num)

    def __iter__(self):
        self._iter_offset = 0
        self._iter_idx = 0
        self._iter_file = self._files[0]

        return self

    def __next__(self):
        chunk, idx, offset = self.__read(self._iter_idx,
                                         self._iter_offset)

        if self._iter_idx == idx and self._iter_offset == offset:
            raise StopIteration()

        self._iter_idx = idx
        self._iter_offset = offset

        return chunk

    def read(self, chunk_num, length=-1, extra_offset=0):
        idx, offset = self.offset(chunk_num, extra_offset)
        return self.__read(idx, offset, length)[0]

    def write(self, chunk_num, data, extra_offset=0):
        idx, offset = self.offset(chunk_num, extra_offset)
        return self.__write(idx, offset, data)[0]

    def __read(self, idx, offset, length=-1):

        read = 0
        chunk = None
        source = self._files[idx]

        if length < 0:
            length = self._chunk_size

        while read < length:

            with self._locks[idx]:
                if source.tell() != offset:
                    source.seek(offset)
                buf = source.read(length - read)

            chunk = chunk + buf if chunk else buf
            read += len(buf)
            offset += len(buf)

            is_enough = len(chunk) == length
            is_end = idx + 1 == len(self._sizes)
            if is_enough or is_end:
                break

            idx += 1
            offset = 0
            source = self._files[idx]

        return chunk, idx, offset

    def __write(self, idx, offset, data):

        written = 0
        chunk = data.__class__(data)
        source = self._files[idx]

        while written < len(data):

            with self._locks[idx]:
                if source.tell() != offset:
                    source.seek(offset)
                write_cnt = self._sizes[idx] - offset
                write_cnt = source.write(chunk[:write_cnt])

            chunk = chunk[write_cnt:]
            written += write_cnt
            offset += write_cnt

            is_enough = written == len(data)
            is_end = idx + 1 == len(self._sizes)
            if is_enough or is_end:
                break

            idx += 1
            offset = 0
            source = self._files[idx]

        return written, idx, offset

