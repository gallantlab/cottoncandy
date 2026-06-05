'''
'''


import os

from cottoncandy import options

from .utils import get_keys, string2bool


__version__ = "0.4.0"

ACCESS_KEY = options.config.get('login', 'access_key')
SECRET_KEY = options.config.get('login', 'secret_key')
ENDPOINT_URL = options.config.get('login', 'endpoint_url')
DEFAULT_SIGNATURE_VERSION = options.config.get('basic', 'signature_version')

default_bucket = options.config.get('basic', 'default_bucket')
force_bucket_creation = options.config.get('basic', 'force_bucket_creation')
force_bucket_creation = string2bool(force_bucket_creation)


def get_interface(bucket_name=None,
                  ACCESS_KEY=None,
                  SECRET_KEY=None,
                  endpoint_url=None,
                  force_bucket_creation=None,
                  verbose=True,
                  backend=None,
                  profile=None,
                  **kwargs):
    """Return an interface to the cloud.

    Parameters
    ----------
    bucket_name : str
    ACCESS_KEY : str
    SECRET_KEY : str
    endpoint_url : str
        The URL for the S3 gateway
    backend : 's3'|'gdrive'|'local'
        What backend to hook on to
    profile : str, optional
        Name of a ``[profile:NAME]`` section in the configuration file from
        which to read the connection settings (access/secret keys, endpoint,
        bucket, signature version, backend, gdrive credentials). Any setting a
        profile omits falls back to the base ``[login]``/``[basic]``/``[gdrive]``
        sections. Explicitly-passed arguments always take precedence over the
        profile. See ``cottoncandy.options.list_profiles``.
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

    # Resolve the profile (falls back to the base config sections). Any
    # argument left as ``None`` is filled in from the resolved settings, so
    # explicitly-passed arguments always win over the profile/config.
    settings = options.get_profile(profile)

    if bucket_name is None:
        bucket_name = settings['default_bucket']
    if ACCESS_KEY is None:
        ACCESS_KEY = settings['access_key']
    if SECRET_KEY is None:
        SECRET_KEY = settings['secret_key']
    if endpoint_url is None:
        endpoint_url = settings['endpoint_url']
    if backend is None:
        backend = settings['backend'] or 's3'
    if force_bucket_creation is None:
        force_bucket_creation = string2bool(settings['force_bucket_creation'])
    signature_version = settings['signature_version']

    if backend == 's3':
        if ACCESS_KEY in [False, "False", None] or SECRET_KEY in [False, "False", None]:
            ACCESS_KEY, SECRET_KEY = get_keys()
    elif backend == 'gdrive':
        ACCESS_KEY = os.path.join(options.userdir, settings['secrets'])
        SECRET_KEY = os.path.join(options.userdir, settings['credentials'])
    else:
        pass

    if 'config' in kwargs:
        # user provided config
        if not kwargs['config'].signature_version:
            # config does not specify signature
            kwargs['config'].signature_version = signature_version
    elif signature_version:
        # no config but default signature exists
        from botocore.client import Config
        kwargs['config'] = Config(signature_version=signature_version)

    interface = DefaultInterface(bucket_name,
                                 ACCESS_KEY,
                                 SECRET_KEY,
                                 endpoint_url,
                                 force_bucket_creation,
                                 verbose=verbose,
                                 backend=backend,
                                 **kwargs)
    return interface


def get_browser(bucket_name=None,
                ACCESS_KEY=None,
                SECRET_KEY=None,
                endpoint_url=None,
                profile=None):
    """Browser object that allows you to tab-complete your
    way through your objects

    Parameters
    ----------
    bucket_name : str
    ACCESS_KEY : str
    SECRET_KEY : str
    endpoint_url : str
        The URL for the S3 gateway
    profile : str, optional
        Name of a ``[profile:NAME]`` section in the configuration file from
        which to read the connection settings. Explicitly-passed arguments take
        precedence over the profile. See ``cottoncandy.options.list_profiles``.

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

    # Resolve the profile (falls back to the base config sections); explicitly
    # passed arguments win over the profile.
    settings = options.get_profile(profile)
    backend = settings['backend'] or 's3'
    if backend != 's3':
        raise ValueError("get_browser only supports the 's3' backend; got %r" % backend)
    if bucket_name is None:
        bucket_name = settings['default_bucket']
    if ACCESS_KEY is None:
        ACCESS_KEY = settings['access_key']
    if SECRET_KEY is None:
        SECRET_KEY = settings['secret_key']
    if endpoint_url is None:
        endpoint_url = settings['endpoint_url']

    if ACCESS_KEY in [False, "False", None] and SECRET_KEY in [False, "False", None]:
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
