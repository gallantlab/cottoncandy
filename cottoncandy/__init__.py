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
encryptionKey = b64decode(options.config.get('encryption', 'key'))

def get_interface(bucket_name=default_bucket,
                  ACCESS_KEY=ACCESS_KEY,
                  SECRET_KEY=SECRET_KEY,
                  endpoint_url=ENDPOINT_URL,
                  force_bucket_creation=force_bucket_creation,
                  verbose=True,
                  backend = 's3'):
    '''Return an interface to S3.

    Parameters
    ----------
    bucket_name : str
        Bucket to use
    ACCESS_KEY : str
        The S3 access key
    SECRET_KEY : str
        The S3 secret key
    url : str
        The URL for the S3 gateway
    backend : 's3'|'gdrive'
        What backend to hook on to

    Returns
    -------
    cci  : cottoncandy.InterfaceObject
        Cottoncandy interface object
    '''
    from .interfaces import DefaultInterface

    if (ACCESS_KEY is False) and (SECRET_KEY is False):
        from .utils import get_keys
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
                                 backend = backend)
    return interface


def get_encrypted_interface(bucket_name=default_bucket,
                          ACCESS_KEY=ACCESS_KEY,
                          SECRET_KEY=SECRET_KEY,
                          endpoint_url=ENDPOINT_URL,
                          force_bucket_creation=force_bucket_creation,
                          verbose=True,
                          backend = 's3', encryption = encryption, encryptionKey = encryptionKey):
    """
    Returns a cc interface that encrypts things
    By default, encryption is 32 bit AES, single key for everything. The key is stored in base64
    in your config file. You can also choose to use RSA-encrypted AES, in which each file gets a
    different key, and the keys are encrypted using RSA and stored alongside each file.

    Parameters
    ----------
    bucket_name
    ACCESS_KEY
    SECRET_KEY
    endpoint_url
    force_bucket_creation
    verbose
    backend
    encryption
    encryptionKey

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
    """Get an object that allows you to tab-complete your
    way through your objects

    Parameters
    ----------
    bucket_name : str
        Bucket to use
    ACCESS_KEY : str
        The S3 access key
    SECRET_KEY : str
        The S3 secret key
    endpoint_url : str
        The URL for the S3 gateway

    Returns
    -------
    ccb  : cottoncandy.BrowserObject
        Cottoncandy browser object

    Example
    -------
    >>> browser = cloud.get_browser('anunez_raid')
    >>> browser.auto.k<TAB-COMPLETION>
    browser.auto.k1
    browser.auto.k8

    >>> browser.auto.k8.anunez.proj.deepnet.caffenet_<TAB-COMPLETION>
    browser.auto.k8.anunez.proj.deepnet.caffenet_ANfs_performance_HDF
    browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF

    >>> browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF
    <h5py-like-group @anunez_raid bucket: caffenet_BGfs_performance.hdf (6 keys)>

    >>> # We can also explore the contents of the HDF-like object
    >>> browser.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.<TAB-COMPLETION>
    browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc6
    browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc7
    browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc8

    >>> # let's look at one
    >>> browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc8
    <h5py-like-dataset @anunez_raid bucket: caffenet_conv3 [shape=(73221)]>

    >>> # we can download the data as an array
    >>> arr = browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc8.load()
    >>> arr.shape
    (73221,)

    >>> # we can also download the object. useful when no class for object exists
    >>> browser.auto.k8.anunez.proj.deepnet.caffenet_BGfs_performance_HDF.caffenet_fc8()
    s3.Object(bucket_name='anunez_raid', key='auto/k8/anunez/proj/deepnet/caffenet_BGfs_performance.hdf/caffenet_fc8')
    """
    from .interfaces import DefaultInterface
    from .browser import S3Directory


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
