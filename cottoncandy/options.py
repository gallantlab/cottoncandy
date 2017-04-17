import os
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


cwd = os.path.split(os.path.abspath(__file__))[0]
userdir = appdirs.user_data_dir("cottoncandy", "aone")
usercfg = os.path.join(userdir, "options.cfg")

config = configparser.ConfigParser()
config.readfp(open(os.path.join(cwd, 'defaults.cfg')))


# case no user config file
if len(config.read(usercfg)) == 0:
    if not os.path.exists(userdir):
        os.makedirs(userdir)

    # write keys to user config file
    ak = config.get("login", "access_key")
    sk = config.get("login", "secret_key")
    if (ak == 'auto') and (sk == 'auto'):
        result = get_keys()
        if result is not None:
            ak, sk = result
        else:
            ak = sk = 'KEYSNOTFOUND'
        config.set("login", "access_key", ak)
        config.set("login", "secret_key", sk)

    aesKey = config.get('encryption', 'key')
    if aesKey == 'auto':
        from Crypto import Random
        newKey = Random.get_random_bytes(32)
        aesKey = b64encode(newKey)
        config.set("encryption", 'key', aesKey)

    with open(usercfg, 'w') as fp:
        config.write(fp)

# add things to old versions of config if needed
else:
    try:	# encryption section
        aesKey = config.get('encryption', 'key')
        if aesKey == 'auto':
            from Crypto import Random
            newKey = Random.get_random_bytes(32)
            aesKey = b64encode(newKey)
            config.set("encryption", 'key', aesKey)
    except configparser.NoSectionError:
        config.add_section('encryption')
        from Crypto import Random
        newKey = Random.get_random_bytes(32)
        aesKey = b64encode(newKey)
        config.set("encryption", 'key', aesKey)
        config.set('encryption', 'method', 'AES')

    try:	# gdrive section
        secrets = config.get('gdrive', 'secrets')
        credentials = config.get('gdrive', 'credentials')
    except configparser.NoSectionError:
        config.add_section('gdrive')
        config.set('gdrive', 'secrets', 'client_secrets.json')
        config.set('gdrive', 'credentials', 'credentials.txt')

    with open(usercfg, 'w') as configfile:
        config.write(configfile)
