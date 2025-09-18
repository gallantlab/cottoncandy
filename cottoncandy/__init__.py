'''
'''

from __future__ import absolute_import

import os
from base64 import b64decode

from cottoncandy import options

from .browser import BrowserObject
from .interfaces import InterfaceObject
from .utils import get_keys, string2bool

__version__ = "0.3.0"

ACCESS_KEY = options.config.get('login', 'access_key')
SECRET_KEY = options.config.get('login', 'secret_key')
ENDPOINT_URL = options.config.get('login', 'endpoint_url')
DEFAULT_SIGNATURE_VERSION = options.config.get('basic', 'signature_version')

default_bucket = options.config.get('basic', 'default_bucket')
force_bucket_creation = options.config.get('basic', 'force_bucket_creation')
force_bucket_creation = string2bool(force_bucket_creation)

def get_interface(bucket_name=default_bucket,
                  ACCESS_KEY=ACCESS_KEY,
                  SECRET_KEY=SECRET_KEY,
                  endpoint_url=ENDPOINT_URL,
                  force_bucket_creation=force_bucket_creation,
                  verbose=True,
                  backend='s3',
                  **kwargs):
    """Return an interface to the cloud.

    Parameters
    ----------
    bucket_name : str
    ACCESS_KEY : str
    SECRET_KEY : str
    endpoint_url : str
        The URL for the S3 gateway
    backend : 's3'|'gdrive'
        What backend to hook on to
    kwargs :
        S3 only. kwargs passed to botocore. For example,
        >>> from botocore.client import Config
        >>> config = Config(connect_timeout=50, read_timeout=10*60)
        >>> cci = cc.get_interface('my_bucket', config=config)

    Returns
    -------
    cci : cottoncandy.InterfaceObject
    """
    from cottoncandy.interfaces import DefaultInterface

    if backend == 's3':
        if ACCESS_KEY in [False, "False"] or SECRET_KEY in [False, "False"]:
            ACCESS_KEY, SECRET_KEY = get_keys()
    elif backend == 'gdrive':
        ACCESS_KEY = os.path.join(options.userdir, options.config.get('gdrive', 'secrets'))
        SECRET_KEY = os.path.join(options.userdir, options.config.get('gdrive', 'credentials'))
    else:
        pass

    if 'config' in kwargs:
        # user provided config
        if not kwargs['config'].signature_version:
            # config does not specify signature
            kwargs['config'].signature_version = DEFAULT_SIGNATURE_VERSION
    elif DEFAULT_SIGNATURE_VERSION:
        # no config but default signature exists
        from botocore.client import Config
        kwargs['config'] = Config(signature_version=DEFAULT_SIGNATURE_VERSION)

    interface = DefaultInterface(bucket_name,
                                 ACCESS_KEY,
                                 SECRET_KEY,
                                 endpoint_url,
                                 force_bucket_creation,
                                 verbose=verbose,
                                 backend = backend,
                                 **kwargs)
    return interface


def get_browser(bucket_name=default_bucket,
                ACCESS_KEY=ACCESS_KEY,
                SECRET_KEY=SECRET_KEY,
                endpoint_url=ENDPOINT_URL):
    """Browser object that allows you to tab-complete your
    way through your objects

    Parameters
    ----------
    bucket_name : str
    ACCESS_KEY : str
    SECRET_KEY : str
    endpoint_url : str
        The URL for the S3 gateway

    Returns
    -------
    ccb : cottoncandy.BrowserObject

    Example
    -------
    >>> browser = cc.get_browser('my_bucket',
                                 ACCESS_KEY='FAKEACCESSKEYTEXT',
                                 SECRET_KEY='FAKESECRETKEYTEXT',
                                 endpoint_url='https://s3.amazonaws.com')
    >>> browser.sweet_project.sub<TAB>
    browser.sweet_project.sub01_awesome_analysis_DOT_grp
    browser.sweet_project.sub02_awesome_analysis_DOT_grp
    >>> browser.sweet_project.sub01_awesome_analysis_DOT_grp
    <cottoncandy-group <bucket:my_bucket_name> (sub01_awesome_analysis.grp: 3 keys)>
    >>> browser.sweet_project.sub01_awesome_analysis_DOT_grp.result_model01
    <cottoncandy-dataset <bucket:my_bucket_name [1.00MB:shape=(10000)]>
    """
    from cottoncandy.browser import S3Directory
    from cottoncandy.interfaces import DefaultInterface

    if (ACCESS_KEY is False) and (SECRET_KEY is False):
        from .utils import get_keys
        ACCESS_KEY, SECRET_KEY = get_keys()

    interface = DefaultInterface(bucket_name,
                                 ACCESS_KEY,
                                 SECRET_KEY,
                                 endpoint_url,
                                 force_bucket_creation=False,
                                 verbose=False)

    return S3Directory('/', interface=interface)

__all__ = ['get_interface', 'get_browser', 'interfaces', 'browser']
