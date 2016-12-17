import os
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
    assert result is not None
    access_key, secret_key = result
    return access_key, secret_key


cwd = os.path.split(os.path.abspath(__file__))[0]
userdir = appdirs.user_data_dir("cottoncandy", "aone")
usercfg = os.path.join(userdir, "options.cfg")

config = configparser.ConfigParser()
config.readfp(open(os.path.join(cwd, 'defaults.cfg')))

if len(config.read(usercfg)) == 0:
    os.makedirs(userdir)

    # write keys to user config file
    ak = config.get("login", "access_key")
    sk = config.get("login", "secret_key")
    if (ak == 'auto') and (sk == 'auto'):
        ak, sk = get_keys()
        config.set("login", "access_key", ak)
        config.set("login", "secret_key", sk)

    with open(usercfg, 'w') as fp:
        config.write(fp)
