import time

import numpy as np

WAIT_TIME = 0.1  # Account for Wasabi lag by waiting N [seconds]


def content_generator():
    orders = ['C','F']
    types = ['float16', 'float32', 'float64',
             'int8', 'int16', 'int32', 'int64',
             'uint8', 'uint16', 'uint32',
             'int','float']

    kinds = ['raw', 'slice', 'nonco']
    for kind in kinds:
        for order in orders:
            for dtype in types:
                print(kind, order, dtype)
                data = np.random.randn(20,10,5)
                data = np.asarray(data, order=order).astype(dtype)

                if kind == 'raw':
                    yield data
                elif kind == 'slice':
                    yield data[...,int(data.shape[0]/2):]
                elif kind == 'nonco':
                    yield data[np.random.randint(0,data.shape[0],10)]


def test_upload_from_file(cci, object_name):
    '''test file uploads'''

    # byte round trip
    content = b'abcdefg123457890'
    flname = '/tmp/test.txt'
    with open(flname, 'wb') as fl:
        fl.write(content)

    print(cci.upload_from_file(flname, object_name=object_name))
    time.sleep(WAIT_TIME)
    dat = cci.download_object(object_name)
    assert dat == content

    # string roundtrip
    content = 'abcdefg123457890'
    flname = '/tmp/test.txt'
    with open(flname, 'w') as fl:
        fl.write(content)

    print(cci.upload_from_file(flname, object_name=object_name))
    time.sleep(WAIT_TIME)
    dat = cci.download_object(object_name).decode()
    assert dat == content
    cci.rm(object_name, recursive=True)


def test_upload_json(cci, object_name):
    content = dict(hello=0,
                   bye='bye!',
                   )

    print(cci.upload_json(object_name, content))
    time.sleep(WAIT_TIME)
    dat = cci.download_json(object_name)
    assert dat == content
    cci.rm(object_name, recursive=True)


def test_pickle_upload(cci, object_name):
    content = dict(hello=1,
                   bye='bye?')

    print(cci.upload_pickle(object_name, content))
    time.sleep(WAIT_TIME)
    dat = cci.download_pickle(object_name)
    assert dat == content
    cci.rm(object_name, recursive=True)


def test_upload_npy_upload(cci, object_name):
    for content in content_generator():
        print(cci.upload_npy_array(object_name, content))
        time.sleep(WAIT_TIME)
        dat = cci.download_npy_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)


def test_upload_raw_array(cci, object_name):
    for i, content in enumerate(content_generator()):
        print(i, cci.upload_raw_array(object_name, content))
        time.sleep(WAIT_TIME)
        dat = cci.download_raw_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)


def test_upload_raw_array_uncompressed(cci, object_name):
    for i, content in enumerate(content_generator()):
        print(i, cci.upload_raw_array(object_name, content, compression=False))
        time.sleep(WAIT_TIME)
        dat = cci.download_raw_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)


def test_upload_dask_array(cci, object_name):
    for content in content_generator():
        print(cci.upload_dask_array(object_name, content))
        time.sleep(WAIT_TIME)
        dat = cci.download_dask_array(object_name)
        dat = np.asarray(dat)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)


def test_dict2cloud(cci, object_name):
    for cc in content_generator():
        content = dict(arr1=cc,
                       deep=dict(dat01=np.random.randn(15),
                                 dat02=np.random.randn(30),
                                 ),
                       )

        print(cci.dict2cloud(object_name, content))
        time.sleep(WAIT_TIME)
        dat = cci.cloud2dict(object_name)
        assert np.allclose(dat['arr1'], content['arr1'])
        for k,v in content['deep'].items():
            assert np.allclose(v, dat['deep'][k])
        cci.rm(object_name, recursive=True)
