import time

import numpy as np

WAIT_TIME = 0.5  # Account for Wasabi lag by waiting N [seconds]


def content_generator():
    size_mb = 101

    orders = ['F', 'C']
    types = [
        'float64',
    ]

    kinds = ['nonco']
    for kind in kinds:
        for order in orders:
            for dtype in types:
                nitems = int(np.ceil(np.sqrt(1 + size_mb * (2 ** 20) / 8)))
                data = np.random.randn(nitems, nitems)
                data = np.asarray(data, order=order, dtype=dtype)

                if kind == 'nonco':
                    mask = np.random.randn(*data.shape) > 2
                    data[mask] = 1
                    data_slice = data

                print(data_slice.flags)
                yield data_slice


def test_upload_raw_array(cci, object_name):
    for content in content_generator():
        print(cci.upload_raw_array(object_name, content, compression='Zstd'))
        time.sleep(WAIT_TIME)
        dat = cci.download_raw_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)
