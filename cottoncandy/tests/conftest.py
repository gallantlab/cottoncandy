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

    # Use github secrets for accessing wasabi from github actions.
    cci = cc.get_interface(
        bucket_name=os.environ['DL_BUCKET_NAME'],
        ACCESS_KEY=os.environ['DL_ACCESS_KEY'],
        SECRET_KEY=os.environ['DL_SECRET_KEY'],
        endpoint_url=os.environ['DL_URL'],
        verbose=False,
    )
    yield cci

    # cleanup the directory entirely
    cci.rm(directory, recursive=True)
