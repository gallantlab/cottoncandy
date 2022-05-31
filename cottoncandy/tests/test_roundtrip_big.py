import time

import numpy as np

WAIT_TIME = 0.1  # Account for Wasabi lag by waiting N [seconds]


def content_generator():
    size_mb = 200
    orders = ['C','F']
    types = ['float64',
             ]

    kinds = ['raw']
    for kind in kinds:
        for order in orders:
            for dtype in types:
                data = np.random.randn(int(1 + size_mb*(2**20)/8))
                data = np.asarray(data, order=order).astype(dtype)

                if kind == 'raw':
                    yield data
                elif kind == 'slice':
                    yield data[data.shape[0]/2:]
                elif kind == 'nonco':
                    yield data[np.random.randint(0,data.shape[0],10)]


def test_upload_raw_array(cci, object_name):
    for content in content_generator():
        print(cci.upload_raw_array(object_name, content, compression=None))
        time.sleep(WAIT_TIME)
        dat = cci.download_raw_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)
