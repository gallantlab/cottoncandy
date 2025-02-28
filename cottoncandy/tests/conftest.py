"""
Define some global variables accessible from all tests (with pytest fixture).
"""
import os
import sys
import tempfile

import pytest
import cottoncandy as cc

directory = 'testcc'

# Check if S3 environment variables are set and not empty
required_env_vars = ['DL_BUCKET_NAME', 'DL_ACCESS_KEY', 'DL_SECRET_KEY', 'DL_URL']
s3_env_vars_present = all(env_var in os.environ and os.environ[env_var] for env_var in required_env_vars)

# Only include S3 client if environment variables are set
all_clients = ["local"]
if s3_env_vars_present:
    all_clients.append("s3")


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
        cci.wait_time = 4.0  # Account for Wasabi lag by waiting N [seconds]
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
