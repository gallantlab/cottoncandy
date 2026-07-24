import configparser
import os

from . import appdirs


def get_key_from_s3fs():
    '''If user has s3fs-fuse keys,return them
    '''
    key_path = os.path.expanduser('~/.passwd-s3fs')
    if os.path.exists(key_path):
        with open(key_path, 'r') as kfl:
            content = kfl.readline()
            ACCESS_KEY, SECRET_KEY = content.strip().split(':')
            return ACCESS_KEY, SECRET_KEY


def get_key_from_environ():
    try:
        ak = os.environ['AWS_ACCESS_KEY'],
        sk = os.environ['AWS_SECRET_KEY']
        return ak, sk
    except KeyError:
        return


def get_keys():
    '''try to find the user keys in the machine
    '''
    # try to outload keys
    resulta = get_key_from_s3fs()
    resultb = get_key_from_environ()
    result = resultb if (resulta is None) else resulta
    return result


def get_config():
    config = configparser.ConfigParser()
    with open(os.path.join(cwd, 'defaults.cfg'), 'r') as defaults_file:
        config.read_file(defaults_file)
    return config

cwd = os.path.split(os.path.abspath(__file__))[0]
userdir = appdirs.user_data_dir("cottoncandy",appauthor="cottoncandy")
usercfg = os.path.join(userdir, "options.cfg")
config = get_config()

# case no user config file
if len(config.read(usercfg)) == 0:
    if not os.path.exists(userdir):
        os.makedirs(userdir)

    prompt = '''
############################################################
Hi! Looks like this is your first time using cottoncandy.

Your cottoncandy configuration file will be stored in:
{path}

You can store your S3 or Google Drive credentials there,
and many other options.

Thanks for using cottoncandy!
'''
    print(prompt.format(path=usercfg))

    with open(usercfg, 'w') as fp:
        config.write(fp)


# add things to old versions of config if needed
else:
    needs_update = False

    try:	# gdrive section
        secrets = config.get('gdrive', 'secrets')
        credentials = config.get('gdrive', 'credentials')
    except configparser.NoSectionError:
        config.add_section('gdrive')
        config.set('gdrive', 'secrets', 'client_secrets.json')
        config.set('gdrive', 'credentials', 'credentials.txt')
        needs_update = True

    if needs_update:
        with open(usercfg, 'w') as configfile:
            config.write(configfile)


# Profiles
# --------
# A profile bundles the connection-identity settings so users can switch
# between accounts/endpoints/buckets via ``get_interface(profile='NAME')``.
# Each profile lives in its own ``[profile:NAME]`` section and may set any
# subset of the keys below. Anything a profile omits falls back to the base
# ``[login]``/``[basic]``/``[gdrive]`` sections (the default profile). A profile
# can inherit from another with ``inherits = PARENT`` and only specify the keys
# it overrides.
PROFILE_PREFIX = 'profile:'
INHERITS_KEY = 'inherits'

# logical name -> (base section, key) used as the default-profile fallback
PROFILE_KEYS = {
    'access_key': ('login', 'access_key'),
    'secret_key': ('login', 'secret_key'),
    'endpoint_url': ('login', 'endpoint_url'),
    'default_bucket': ('basic', 'default_bucket'),
    'signature_version': ('basic', 'signature_version'),
    'force_bucket_creation': ('basic', 'force_bucket_creation'),
    'backend': ('basic', 'backend'),
    'secrets': ('gdrive', 'secrets'),
    'credentials': ('gdrive', 'credentials'),
}


def list_profiles(cfg=None):
    '''List the names of the profiles defined in the configuration.

    Parameters
    ----------
    cfg : configparser.ConfigParser, optional
        Defaults to the global cottoncandy config.

    Returns
    -------
    profiles : list of str
    '''
    cfg = config if cfg is None else cfg
    return [section[len(PROFILE_PREFIX):] for section in cfg.sections()
            if section.startswith(PROFILE_PREFIX)]


def _profile_chain(name, cfg, _seen=None):
    '''Return the inheritance chain for a profile, root parent first.

    Raises
    ------
    ValueError
        If the profile (or a parent) does not exist, or if the ``inherits``
        relationships form a cycle.
    '''
    _seen = [] if _seen is None else _seen
    section = PROFILE_PREFIX + name
    if not cfg.has_section(section):
        raise ValueError('Unknown cottoncandy profile %r. Available: %s'
                         % (name, list_profiles(cfg)))
    if name in _seen:
        raise ValueError('Circular profile inheritance: %s'
                         % ' -> '.join(_seen + [name]))
    _seen = _seen + [name]
    if cfg.has_option(section, INHERITS_KEY):
        parent = cfg.get(section, INHERITS_KEY).strip()
        if parent:
            return _profile_chain(parent, cfg, _seen) + [name]
    return [name]


def get_profile(name=None, cfg=None):
    '''Resolve a profile into a dict of connection settings.

    Values are resolved most-specific-last: the base ``[login]``/``[basic]``/
    ``[gdrive]`` sections provide the defaults, then each profile in the
    inheritance chain (root parent first) overlays the keys it sets.

    Parameters
    ----------
    name : str or None
        Profile name. ``None`` returns the base/default settings.
    cfg : configparser.ConfigParser, optional
        Defaults to the global cottoncandy config.

    Returns
    -------
    settings : dict
        Keys are those of ``PROFILE_KEYS``; missing values are ``None``.
    '''
    cfg = config if cfg is None else cfg
    settings = {key: (cfg.get(section, option)
                      if cfg.has_option(section, option) else None)
                for key, (section, option) in PROFILE_KEYS.items()}
    if name:
        for profile_name in _profile_chain(name, cfg):
            section = PROFILE_PREFIX + profile_name
            for key in PROFILE_KEYS:
                if cfg.has_option(section, key):
                    settings[key] = cfg.get(section, key)
    return settings
