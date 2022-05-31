"""
Define some global variables accessible from all tests (with pytest fixture).
"""
import os
import sys

import pytest
import cottoncandy as cc

directory = 'testcc'


@pytest.fixture(scope="session")
def object_name():
    name = os.path.join(directory, f"py{sys.version[:6]}", 'test')
    return name


@pytest.fixture(scope="session")
def cci():

    # for accessing wasabi from github actions.
    bucket_name = os.environ['DL_BUCKET_NAME']
    AK = os.environ['DL_ACCESS_KEY']
    SK = os.environ['DL_SECRET_KEY']
    URL = os.environ['DL_URL']

    cci = cc.get_interface(bucket_name, ACCESS_KEY=AK, SECRET_KEY=SK,
                           endpoint_url=URL, verbose=False)
    yield cci

    # cleanup the directory entirely
    cci.rm(directory, recursive=True)
