import hashlib
import os
import shutil
import tempfile

import pytest

from golem.resources.partition import Partition


@pytest.fixture(scope='function')
def temp_dir(request):
    """ Creates a temp directory with test's lifetime  """
    tmp = tempfile.mkdtemp()
    request.addfinalizer(lambda: shutil.rmtree(tmp))
    return tmp

def _create_paths(directory, name, count):
    subdirectory = os.path.join(directory, name)
    os.makedirs(subdirectory)

    paths = []

    for i in range(count):
        path = os.path.join(subdirectory, 'file_{}'.format(i))
        paths.append(path)

    return paths

def _allocate_files(paths, sizes, data=None):
    for path, size in zip(paths, sizes):
        written = 0

        with open(path, 'wb') as out:
            while written < size:
                length = min(65536, size - written)
                buf = os.urandom(length) if data is None else data * length
                written += out.write(buf)

def _create_files(directory, name, sizes, data=None):
    paths = _create_paths(directory, name, len(sizes))
    _allocate_files(paths, sizes, data)
    return paths

def _sha256_file(path):
    sha256 = hashlib.sha256()

    with open(path, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest()


def test_iteration_copy(temp_dir):
    sizes = [0, 1, 7, 9, 10, 13, 100, 157, 1000, 1096, 2000900, 10101037]

    paths_0 = _create_files(temp_dir, 'partition_0', sizes)
    partition_0 = Partition(paths_0)

    paths_1 = _create_paths(temp_dir, 'partition_1', len(sizes))
    partition_1 = Partition.allocate(paths_1, sizes)

    partition_0.open()
    partition_1.open()

    try:
        for i, chunk in enumerate(partition_0):
            partition_1[i] = chunk
    finally:

        partition_0.close()
        partition_1.close()

    for path_0, path_1 in zip(paths_0, paths_1):
        assert _sha256_file(path_0) == _sha256_file(path_1)


def test_indexed_copy(temp_dir):
    sizes = [32145, 123, 1252336, 0, 412123, 213, 532390, 12]

    paths_0 = _create_files(temp_dir, 'partition_0', sizes)
    partition_0 = Partition(paths_0)

    paths_1 = _create_paths(temp_dir, 'partition_1', len(sizes))
    partition_1 = Partition.allocate(paths_1, sizes)

    partition_0.open()
    partition_1.open()

    try:
        for i in range(partition_0.size()):
            partition_1[i] = partition_0[i]
    finally:

        partition_0.close()
        partition_1.close()

    for path_0, path_1 in zip(paths_0, paths_1):
        assert _sha256_file(path_0) == _sha256_file(path_1)

def test_stream_copy(temp_dir):
    sizes = [32145, 123, 1252336, 0, 412123, 213, 532390, 12]

    paths_0 = _create_files(temp_dir, 'partition_0', sizes)
    partition_0 = Partition(paths_0)

    paths_1 = _create_paths(temp_dir, 'partition_1', len(sizes))
    partition_1 = Partition.allocate(paths_1, sizes)

    partition_0.open()
    partition_1.open()

    try:
        for i in range(partition_0.size()):
            data = partition_0.get_stream(i).read(-1)
            partition_1.get_stream(i).write(data)
    finally:

        partition_0.close()
        partition_1.close()

    for path_0, path_1 in zip(paths_0, paths_1):
        assert _sha256_file(path_0) == _sha256_file(path_1)

