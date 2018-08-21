import os
import sys
import pwd

from base64 import b64decode, b64encode

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

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
    except:
        return


def get_keys():
    '''try to find the user keys in the machine
    '''
    # try to outload keys
    resulta = get_key_from_s3fs()
    resultb = get_key_from_environ()
    result = resultb if (resulta is None) else resulta
    return result

def generate_AES_key(bytes = 32):
    """Generates a new AES key

    Parameters
    ----------
    bytes : int
        number of bytes in key

    Returns
    -------
    key : bytes
    """
    try:
        from Crypto import Random
        return Random.get_random_bytes(bytes)
    except ImportError:
        print('PyCrypto not install. Reading from /dev/random instead')
        with open('/dev/random', 'r') as rand:
            return rand.read(bytes)


def get_config():
    config = configparser.ConfigParser()
    defaults_file = open(os.path.join(cwd, 'defaults.cfg'), 'r')
    if sys.version_info.major == 2:
        config.readfp(defaults_file)
    else:
        config.read_file(defaults_file)
    return config

cwd = os.path.split(os.path.abspath(__file__))[0]
userdir = appdirs.user_data_dir("cottoncandy")
usercfg = os.path.join(userdir, "options.cfg")
config = get_config()

# case no user config file
if len(config.read(usercfg)) == 0:
    if not os.path.exists(userdir):
        os.makedirs(userdir)

    aesKey = config.get('encryption', 'key')
    if aesKey == 'auto':
        newKey = generate_AES_key()
        aesKey = str(b64encode(newKey))
        config.set("encryption", 'key', aesKey)

    with open(usercfg, 'w') as fp:
        config.write(fp)


# add things to old versions of config if needed
else:
    needs_update = False
    try:	# encryption section
        aesKey = config.get('encryption', 'key')
        if aesKey == 'auto':
            aesKey = str(b64encode(generate_AES_key()))
            config.set("encryption", 'key', aesKey)
            needs_update = True
    except configparser.NoSectionError:
        config.add_section('encryption')
        newKey = generate_AES_key()
        aesKey = str(b64encode(newKey))
        config.set("encryption", 'key', aesKey)
        config.set('encryption', 'method', 'AES')
        needs_update = True

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
