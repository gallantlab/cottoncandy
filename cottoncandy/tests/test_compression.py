import os
import sys
import time
import datetime

import numpy as np

import cottoncandy as cc



##############################
# globals
##############################
WAIT_TIME = 2.         # Account for Wasabi lag by waiting N [seconds]
DATE = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')

prefix = 'testcc/py%s'%(DATE, sys.version[:6])
object_name = os.path.join(prefix, 'test')


##############################
# login
##############################

if True:
    # for accessing wasabi from github actions.
    bucket_name = os.environ['DL_BUCKET_NAME']
    AK = os.environ['DL_ACCESS_KEY']
    SK = os.environ['DL_SECRET_KEY']
    URL = os.environ['DL_URL']

    cci = cc.get_interface(bucket_name,
                           ACCESS_KEY=AK,
                           SECRET_KEY=SK,
                           endpoint_url=URL,
                           verbose=False)
else:
    ##############################
    # Warning
    ##############################
    # This will use your defaults to run the tests on.
    # If you use AWS, you might incur costs.
    cci = cc.get_interface()


##############################
# tests
##############################

def content_generator():
    size_mb = 101

    orders = ['F','C']
    types = ['float64',
             ]

    kinds = ['nonco']
    for kind in kinds:
        for order in orders:
            for dtype in types:
                nitems = int(np.ceil(np.sqrt(1 + size_mb*(2**20)/8)))
                data = np.random.randn(nitems, nitems)
                data = np.asarray(data, order=order, dtype=dtype)

                if kind == 'nonco':
                    mask = np.random.randn(*data.shape) > 2
                    data[mask] = 1
                    data_slice = data

                print(data_slice.flags)
                yield data_slice


def test_upload_raw_array():
    for content in content_generator():
        print(cci.upload_raw_array(object_name, content, compression='Zstd'))
        time.sleep(WAIT_TIME)
        dat = cci.download_raw_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)
