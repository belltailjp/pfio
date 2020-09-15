from pfio.cache import FileCache
from pfio.cache import MultiprocessFileCache
import os
import tempfile

import pytest


def test_enospc(monkeypatch):
    def mock_pread(_fd, _buf, _offset):
        ose = OSError(28, "No space left on device")
        raise ose
    with monkeypatch.context() as m:
        m.setattr(os, 'pread', mock_pread)

        with FileCache(10) as cache:
            i = 2
            with pytest.warns(RuntimeWarning):
                cache.put(i, str(i))


def test_enoent(monkeypatch):
    def mock_pread(_fd, _buf, _offset):
        ose = OSError(2, "No such file or directory")
        raise ose
    with monkeypatch.context() as m:
        m.setattr(os, 'pread', mock_pread)

        with FileCache(10) as cache:

            with pytest.raises(OSError):
                cache.put(4, str(4))


def test_preservation():
    with tempfile.TemporaryDirectory() as d:
        cache = FileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        # Support preserving multiple times with both alive
        cache.preserve('preserved1')
        cache.preserve('preserved2')

        for i in range(10):
            assert str(i) == cache.get(i)

        cache.close()

        # Imitating a new process, fresh load
        cache2 = FileCache(10, dir=d, do_pickle=True)
        cache3 = FileCache(10, dir=d, do_pickle=True)

        cache2.preload('preserved1')
        cache3.preload('preserved2')
        for i in range(10):
            assert str(i) == cache2.get(i)
            assert str(i) == cache2.get(i)


def test_preservation_multiple_times():
    with tempfile.TemporaryDirectory() as d:
        cache = FileCache(1, dir=d, do_pickle=True)
        cache.put(0, 'hello')
        cache.preserve('preserved')
        cache.preserve('preserved')


def test_preservation_overwrite():
    with tempfile.TemporaryDirectory() as d:
        cache = FileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        # Create a dummy file
        with open(os.path.join(d, 'preserved.cachei'), 'wt') as f:
            f.write('hello')

        with pytest.raises(FileExistsError):
            cache.preserve('preserved')

        cache.preserve('preserved', overwrite=True)


def test_preservation_interoperability():
    with tempfile.TemporaryDirectory() as d:
        cache = FileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        cache.preserve('preserved')

        for i in range(10):
            assert str(i) == cache.get(i)

        cache.close()

        cache2 = MultiprocessFileCache(10, dir=d, do_pickle=True)

        cache2.preload('preserved')
        for i in range(10):
            assert str(i) == cache2.get(i)
