import os
import sys
import time

import numpy as np

import cottoncandy as cc

##############################
# globals
##############################

prefix = 'testcc_py%s'%sys.version[:6]
object_name = os.path.join(prefix, 'test')

bucket_name = os.environ['DL_BUCKET_NAME']
AK = os.environ['DL_ACCESS_KEY']
SK = os.environ['DL_SECRET_KEY']
URL = os.environ['DL_URL']

cci = cc.get_interface(bucket_name,
                       ACCESS_KEY=AK,
                       SECRET_KEY=SK,
                       endpoint_url=URL,
                       verbose=False)




##############################
# tests
##############################

def content_generator():
    orders = ['C','F']
    types = ['float64',
             ]

    kinds = ['raw']
    for kind in kinds:
        for order in orders:
            for dtype in types:
                data = np.random.randn(1 + 200*(2**20)/8)
                data = np.asarray(data, order=order).astype(dtype)

                if kind == 'raw':
                    yield data
                elif kind == 'slice':
                    yield data[data.shape[0]/2:]
                elif kind == 'nonco':
                    yield data[np.random.randint(0,data.shape[0],10)]

def test_upload_raw_array():
    for content in content_generator():
        print(cci.upload_raw_array(object_name, content, gzip=False))
        time.sleep(1.0)
        dat = cci.download_raw_array(object_name)
        assert np.allclose(dat, content)
        cci.rm(object_name, recursive=True)
