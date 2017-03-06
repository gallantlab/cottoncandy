'''
'''

from __future__ import absolute_import
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

def get_interface(bucket_name=default_bucket,
                  ACCESS_KEY=ACCESS_KEY,
                  SECRET_KEY=SECRET_KEY,
                  endpoint_url=ENDPOINT_URL,
                  force_bucket_creation=force_bucket_creation,
                  verbose=True):
    """Return an interface to S3.

    Parameters
    ----------
    bucket_name : str
    ACCESS_KEY : str
    SECRET_KEY : str
    endpoint_url : str
        The URL for the S3 gateway
    force_bucket_creation : bool
        Create requested bucket if it doesn't exist

    Returns
    -------
    cci  : cottoncandy.InterfaceObject
        cottoncandy interface object
    """
    from cottoncandy.interfaces import DefaultInterface

    if (ACCESS_KEY is False) and (SECRET_KEY is False):
        from cottoncandy.utils import get_keys
        ACCESS_KEY, SECRET_KEY = get_keys()

    interface = DefaultInterface(bucket_name,
                                 ACCESS_KEY,
                                 SECRET_KEY,
                                 endpoint_url,
                                 force_bucket_creation,
                                 verbose=verbose)
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
    ccb  : cottoncandy.BrowserObject
        cottoncandy browser object

    Example
    -------
    >>> browser = cc.get_browser('my_bucket_name',
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

__all__ = ['get_interface', 'get_browser', 'interfaces', 'browser']
