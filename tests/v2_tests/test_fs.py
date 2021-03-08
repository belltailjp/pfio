# Test fs.FS compatibility
import contextlib
import io
import multiprocessing as mp
import os
import random
import string
import tempfile

from moto import mock_s3
from parameterized import parameterized

from pfio.testing import ZipForTest
from pfio.v2 import S3, Local, from_url, lazify, open_url


def randstring():
    letters = string.ascii_letters + string.digits
    return (''.join(random.choice(letters) for _ in range(16)))


@contextlib.contextmanager
def gen_fs(target):
    if target == "s3":
        bucket = "test-dummy-bucket"
        with S3(bucket, create_bucket=True) as s3:
            yield s3
            # s3.client.delete_bucket(bucket)

    elif target == "local":
        with tempfile.TemporaryDirectory() as d:
            yield Local(d)

    else:
        raise RuntimeError()


@parameterized.expand(["s3", "local"])
@mock_s3
def test_smoke(target):
    filename = randstring()
    content = randstring()
    with gen_fs(target) as fs:
        with fs.open(filename, 'w') as fp:
            fp.write(content)

        with fs.open(filename, 'r') as fp:
            assert content == fp.read()

        assert filename in list(fs.list())

        fs.mkdir('d')

        with fs.open('d/foo', 'w') as fp:
            fp.write(content + content)

        with fs.open('d/foo', 'r') as fp:
            assert (content + content) == fp.read()

        print('recursive:', list(fs.list(recursive=True)))
        print('non-rec:', list(fs.list(recursive=False)))
        assert filename in list(fs.list())
        assert 2 == len(list(fs.list(recursive=False)))

        assert 'd/' in list(fs.list(recursive=False))

        assert 'foo' in list(fs.list('d/'))

        fs.remove(filename)


@mock_s3
def test_factory_open():
    assert isinstance(from_url('.'), Local)
    with open_url('./setup.cfg') as fp:
        assert isinstance(fp, io.IOBase)

    bucket = 'foobar'
    with S3(bucket, create_bucket=True) as s3:
        with s3.open('baz.txt', 'w') as fp:
            fp.write('bom')

        with s3.open('baz.txt', 'r') as fp:
            assert 'bom' == fp.read()

    assert isinstance(from_url('s3://foobar/boom/bom'), S3)

    with open_url('s3://foobar/boom/bom.txt', 'w') as fp:
        fp.write('hello')

    with open_url('s3://foobar/boom/bom.txt', 'r') as fp:
        assert 'hello' == fp.read()


def test_recreate():

    with tempfile.TemporaryDirectory() as d:
        zipfilename = os.path.join(d, "test.zip")
        z = ZipForTest(zipfilename)
        barrier = mp.Barrier(1)

        with lazify(lambda: from_url(zipfilename)) as f:
            with f.open('file', 'rb') as fp:
                content = fp.read()
                assert content
                assert z.content('file') == content

            def func():
                # accessing the shared container
                with f.open('file', 'rb') as fp:
                    barrier.wait()
                    assert content == fp.read()

            p = mp.Process(target=func)
            p.start()

            p.join(timeout=1)
            assert p.exitcode == 0