import gzip
import io

import pytest

from ..utils import GzipInputStream


def make_stream() -> GzipInputStream:
    return GzipInputStream(io.BytesIO(gzip.compress(b'0123456789' * 10)))


def test_gzip_stream_unsupported_seeks_raise_unsupported_operation():
    """Backward and end-relative seeks raise io.UnsupportedOperation.

    Readers that catch UnsupportedOperation (and then buffer the stream)
    can handle it, and the error is still an OSError (IOError) and a
    ValueError, so existing handlers keep working.
    """
    stream = make_stream()
    assert stream.read(20) == b'0123456789' * 2

    with pytest.raises(io.UnsupportedOperation):
        stream.seek(0)
    with pytest.raises(io.UnsupportedOperation):
        stream.seek(-5, io.SEEK_CUR)
    with pytest.raises(io.UnsupportedOperation):
        stream.seek(0, io.SEEK_END)
    with pytest.raises(IOError):
        stream.seek(0)

    # forward seeks still work
    assert stream.seek(30) == 30
    assert stream.read(10) == b'0123456789'
    assert stream.tell() == 40
