#!/usr/bin/env python
import sys

try:
    import configparser
except ImportError:
    import ConfigParser as configparser


from setuptools import setup
from setuptools.command.install import install

def set_default_options(optfile):
    import os
    import pwd

    from cottoncandy import appdirs
    config = configparser.ConfigParser()
    config.read(optfile)
    configdir = appdirs.user_data_dir('cottoncandy')
    usercfg = os.path.join(configdir, "options.cfg")
    issudo = os.getenv('SUDO_USER')
    uname = os.getenv('SUDO_USER') if issudo else os.environ['USER']

    # If it has been previously installed do not overwrite
    if not os.path.exists(usercfg):
        with open(usercfg, 'w') as fp:
            config.write(fp)

    uid = pwd.getpwnam(uname).pw_uid
    gid = pwd.getpwnam(uname).pw_gid

    os.chown(usercfg, uid, gid)
    os.chmod(usercfg, 0o600)
    os.chown(configdir, uid, gid)
    print('cottoncandy configuration path: %s'%usercfg)



class my_install(install):
    def run(self):
        install.run(self)
        optfile = [f for f in self.get_outputs() if 'defaults.cfg' in f]
        set_default_options(optfile[0])


if not 'extra_setuptools_args' in globals():
    extra_setuptools_args = dict()

long_description = """
A python scientific library for storing and accessing numpy array data on S3. This is achieved by reading arrays from memory and downloading arrays directly into memory. This means that you don't have to download your array to disk, and then load it from disk into your python session."""

def main(**kwargs):
    setup(name="""cottoncandy""",
          version='0.1.0rc1',
          description="""sugar for S3""",
          author='Anwar O. Nunez-Elizalde',
          author_email='anwarnunez@gmail.com',
          url='http://gallantlab.github.io/cottoncandy/',
          packages=['cottoncandy',
                    ],
          package_data={
              'cottoncandy':[
                  'defaults.cfg',
                ],
              },
          cmdclass=dict(install=my_install),
          include_package_data=True,
          long_description=long_description,
          **kwargs)

if __name__ == "__main__":
    main(**extra_setuptools_args)
