"""
Define some global variables accessible from all tests (with pytest fixture).
"""
import os
import sys
import tempfile

import pytest
import cottoncandy as cc

directory = 'testcc'
all_clients = ["local", "s3"]


@pytest.fixture(scope="session")
def object_name():
    name = os.path.join(directory, f"py{sys.version[:6]}", 'test')
    return name


@pytest.fixture(scope="session", params=all_clients)
def cci(request):

    if request.param == "s3":
        # Use github secrets for accessing wasabi from github actions.
        cci = cc.get_interface(
            bucket_name=os.environ['DL_BUCKET_NAME'],
            ACCESS_KEY=os.environ['DL_ACCESS_KEY'],
            SECRET_KEY=os.environ['DL_SECRET_KEY'],
            endpoint_url=os.environ['DL_URL'],
            backend="s3",
            verbose=False,
        )
        cci.wait_time = 2.0  # Account for Wasabi lag by waiting N [seconds]
        yield cci

    elif request.param == "local":
        cci = cc.get_interface(
            bucket_name=os.path.join(tempfile.gettempdir(), "cottoncandy"),
            backend="local",
            verbose=False,
        )
        cci.wait_time = 0.001
        yield cci

    # cleanup the directory entirely
    cci.rm(directory, recursive=True)
