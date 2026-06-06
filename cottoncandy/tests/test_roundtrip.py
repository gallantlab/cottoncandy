import os
import tempfile
import time

import numpy as np
import pytest

scipy = pytest.importorskip("scipy")
from scipy import sparse


def content_generator():
    orders = ['C', 'F']
    types = [
        'float16', 'float32', 'float64', 'int8', 'int16', 'int32', 'int64',
        'uint8', 'uint16', 'uint32', 'int', 'float', 'bool'
    ]

    kinds = ['raw', 'slice', 'nonco']
    for kind in kinds:
        for order in orders:
            for dtype in types:
                print(kind, order, dtype)
                data = np.random.randn(20, 10, 5)
                data = np.asarray(data, order=order).astype(dtype)

                if kind == 'raw':
                    yield data
                elif kind == 'slice':
                    yield data[..., int(data.shape[0] / 2):]
                elif kind == 'nonco':
                    yield data[np.random.randint(0, data.shape[0], 10)]


def sparse_content_generator():
    types = [
        'float32', 'float64', 'int8', 'int16', 'int32', 'int64',
        'uint8', 'uint16', 'uint32', 'int', 'float', 'bool'
    ]

    formats = [
        ("csr", lambda arr: sparse.csr_matrix(arr)),
        ("coo", lambda arr: sparse.coo_matrix(arr)),
        ("csc", lambda arr: sparse.csc_matrix(arr)),
        ("bsr", lambda arr: sparse.bsr_matrix(arr, blocksize=(2, 2))),
        ("dia", lambda arr: sparse.dia_matrix(arr)),
        ("lil", lambda arr: sparse.lil_matrix(arr)),
        ("dok", lambda arr: sparse.dok_matrix(arr)),
    ]

    for dtype in types:
        data = np.random.randn(20, 20).astype(dtype)

        # Apply a sparsity mask to the data
        mask = np.random.rand(*data.shape) < 0.8  # 80% sparsity
        sparse_data = np.where(mask, 0, data)

        for format_name, factory in formats:
            print(format_name)
            yield format_name, factory(sparse_data)


def test_upload_from_file(cci, object_name):
    '''test file uploads'''

    # byte round trip
    content = b'abcdefg123457890'
    fd, flname = tempfile.mkstemp(suffix='.txt')
    os.close(fd)  # Close the file descriptor
    with open(flname, 'wb') as fl:
        fl.write(content)

    print(cci.upload_from_file(flname, object_name=object_name))
    time.sleep(cci.wait_time)
    dat = cci.download_object(object_name)
    assert dat == content

    # Clean up temporary file
    try:
        os.unlink(flname)
    except FileNotFoundError:
        pass

    # string roundtrip
    content = 'abcdefg123457890'
    fd, flname = tempfile.mkstemp(suffix='.txt')
    os.close(fd)  # Close the file descriptor
    with open(flname, 'w') as fl:
        fl.write(content)

    print(cci.upload_from_file(flname, object_name=object_name))
    time.sleep(cci.wait_time)
    dat = cci.download_object(object_name).decode()
    assert dat == content
    cci.rm(object_name, recursive=True)

    # Clean up temporary file
    try:
        os.unlink(flname)
    except FileNotFoundError:
        pass


def test_upload_json(cci, object_name):
    content = dict(
        hello=0,
        bye='bye!',
    )

    print(cci.upload_json(object_name, content))
    time.sleep(cci.wait_time)
    dat = cci.download_json(object_name)
    assert dat == content
    cci.rm(object_name, recursive=True)


def test_pickle_upload(cci, object_name):
    content = dict(hello=1, bye='bye?')

    print(cci.upload_pickle(object_name, content))
    time.sleep(cci.wait_time)
    dat = cci.download_pickle(object_name)
    assert dat == content
    cci.rm(object_name, recursive=True)


def test_upload_npy_upload(cci, object_name):
    for content in content_generator():
        print(cci.upload_npy_array(object_name, content))
        time.sleep(cci.wait_time)
        dat = cci.download_npy_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)


def test_upload_raw_array(cci, object_name):
    for i, content in enumerate(content_generator()):
        print(i, cci.upload_raw_array(object_name, content))
        time.sleep(cci.wait_time)
        dat = cci.download_raw_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)


def test_upload_raw_array_uncompressed(cci, object_name):
    for i, content in enumerate(content_generator()):
        print(i, cci.upload_raw_array(object_name, content, compression=False))
        time.sleep(cci.wait_time)
        dat = cci.download_raw_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)


def test_upload_dask_array(cci, object_name):
    for content in content_generator():
        print(cci.upload_dask_array(object_name, content))
        time.sleep(cci.wait_time)
        dat = cci.download_dask_array(object_name)
        dat = np.asarray(dat)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)


def test_dict2cloud(cci, object_name):
    for cc in content_generator():
        content = dict(
            arr1=cc,
            deep=dict(
                dat01=np.random.randn(15),
                dat02=np.random.randn(30),
            ),
        )

        print(cci.dict2cloud(object_name, content))
        time.sleep(cci.wait_time)
        dat = cci.cloud2dict(object_name)
        assert np.allclose(dat['arr1'], content['arr1'])
        for k, v in content['deep'].items():
            assert np.allclose(v, dat['deep'][k])
        cci.rm(object_name, recursive=True)


def test_copy(cci, object_name):
    # Tests that the object contents _and_ metadata are copied correctly
    dest_object_name = object_name + '_temp'
    for content in content_generator():
        cci.upload_raw_array(object_name, content)
        time.sleep(cci.wait_time)
        cci.cp(object_name, dest_object_name, overwrite=True)
        assert cci.exists_object(object_name)
        dat = cci.download_raw_array(dest_object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name)
        cci.rm(dest_object_name)


def test_move(cci, object_name):
    # Tests that the object contents _and_ metadata are moved correctly
    dest_object_name = object_name + '_temp'
    for content in content_generator():
        cci.upload_raw_array(object_name, content)
        time.sleep(cci.wait_time)
        cci.mv(object_name, dest_object_name, overwrite=True)
        assert not cci.exists_object(object_name)
        dat = cci.download_raw_array(dest_object_name)
        assert np.allclose(dat, content)
        cci.rm(dest_object_name)


def test_sparse_roundtrip_matrix_types(cci, object_name):
    for idx, (format_name, matrix) in enumerate(sparse_content_generator()):
        sparse_name = cci.pathjoin(object_name, f"sparse_{format_name}_{idx}")

        cci.upload_sparse_array(sparse_name, matrix)
        time.sleep(cci.wait_time)

        downloaded = cci.download_sparse_array(sparse_name)
        if not isinstance(matrix, (sparse.dok_matrix, sparse.lil_matrix)):
            # DOK and LIL formats are converted to CSR format during upload, so we can't expect the same type back
            assert isinstance(downloaded, type(matrix))
        assert downloaded.shape == matrix.shape
        assert np.allclose(downloaded.toarray(), matrix.toarray())

        cci.rm(sparse_name, recursive=True)
