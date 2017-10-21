'''
'''

from __future__ import absolute_import
import os
from base64 import b64decode
__all__ = []

from .browser import BrowserObject
from .interfaces import InterfaceObject
from .utils import string2bool

from . import options

ACCESS_KEY = options.config.get('login', 'access_key')
SECRET_KEY = options.config.get('login', 'secret_key')
ENDPOINT_URL = options.config.get('login', 'endpoint_url')

default_bucket = options.config.get('basic', 'default_bucket')
force_bucket_creation = options.config.get('basic', 'force_bucket_creation')
force_bucket_creation = string2bool(force_bucket_creation)

encryption = options.config.get('encryption', 'method')
key = options.config.get('encryption', 'key')
if key[-4:] == '.txt':
    with open(os.path.join(options.userdir, key), 'r') as keyfile:
        encryptionKey = b64decode(keyfile.readline())
else:
    encryptionKey = b64decode(key)

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

    if (ACCESS_KEY is False) and (SECRET_KEY is False):
        from cottoncandy.utils import get_keys
        ACCESS_KEY, SECRET_KEY = get_keys()

    if backend == 'gdrive':
        ACCESS_KEY = os.path.join(options.userdir, options.config.get('gdrive', 'secrets'))
        SECRET_KEY = os.path.join(options.userdir, options.config.get('gdrive', 'credentials'))

    interface = DefaultInterface(bucket_name,
                                 ACCESS_KEY,
                                 SECRET_KEY,
                                 endpoint_url,
                                 force_bucket_creation,
                                 verbose=verbose,
                                 backend = backend,
                                 **kwargs)
    return interface


def get_encrypted_interface(bucket_name=default_bucket,
                            ACCESS_KEY=ACCESS_KEY,
                            SECRET_KEY=SECRET_KEY,
                            endpoint_url=ENDPOINT_URL,
                            force_bucket_creation=force_bucket_creation,
                            verbose=True,
                            backend='s3',
                            encryption=encryption,
                            encryptionKey=encryptionKey):
    """
    Returns a cc interface that encrypts things
    By default, encryption is 32 bit AES, single key for everything. The key is stored in base64
    in your config file. You can also choose to use RSA-encrypted AES, in which each file gets a
    different key, and the keys are encrypted using RSA and stored alongside each file.

    Parameters
    ----------
    encryption : str
        'RSA' | 'AES'
    encryptionKey : str
        if RSA, path to key file, if AES, binary string encryption key

    Returns
    -------

    """
    from .interfaces import EncryptedInterface
    if (ACCESS_KEY is False) and (SECRET_KEY is False):
        from .utils import get_keys
        ACCESS_KEY, SECRET_KEY = get_keys()

    if backend == 'gdrive':
        ACCESS_KEY = os.path.join(options.userdir, options.config.get('gdrive', 'secrets'))
        SECRET_KEY = os.path.join(options.userdir, options.config.get('gdrive', 'credentials'))

    interface = EncryptedInterface(bucket_name, ACCESS_KEY, SECRET_KEY, endpoint_url,
                                   encryption, encryptionKey, force_bucket_creation = force_bucket_creation,
                                   verbose = verbose, backend = backend)
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
    from cottoncandy.interfaces import DefaultInterface
    from cottoncandy.browser import S3Directory

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

__all__ = ['get_interface', 'get_encrypted_interface', 'get_browser', 'interfaces', 'browser']
