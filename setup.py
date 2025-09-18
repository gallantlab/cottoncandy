#!/usr/bin/env python
import re
import sys

try:
    import configparser
except ImportError:
    import ConfigParser as configparser


if len(set(('develop', 'bdist_wheel', 'bdist_egg', 'bdist_rpm', 'bdist',
            'sdist', 'bdist_wheel', 'bdist_dumb',
            'bdist_wininst', 'install_egg_info', 'egg_info', 'easy_install')).intersection(sys.argv)) > 0:
    # monkey patch distutils
    from setuptools import setup
    from setuptools.command.install import install
else:
    # use standard library
    from distutils.command.install import install
    from distutils.core import setup

# get version from cottoncandy/__init__.py
__version__ = 0.0
with open('cottoncandy/__init__.py') as f:
    infos = f.readlines()
for line in infos:
    if "__version__" in line:
        match = re.search(r"__version__ = ['\"]([^'\"]*)['\"]", line)
        __version__ = match.groups()[0]


def set_default_options(optfile):
    import os
    import pwd

    config = configparser.ConfigParser()
    config.read(optfile)
    with open(optfile, 'w') as fp:
        config.write(fp)
    print('cottoncandy configuration file: %s'%optfile)


class my_install(install):
    def run(self):
        install.run(self)
        optfile = [f for f in self.get_outputs() if 'defaults.cfg' in f]
        set_default_options(optfile[0])


if 'extra_setuptools_args' not in globals():
    extra_setuptools_args = dict()

long_description = """
A python scientific library for storing and accessing numpy array data on S3. This is achieved by reading arrays from memory and downloading arrays directly into memory. This means that you don't have to download your array to disk, and then load it from disk into your python session."""

def main(**kwargs):
    setup(name="""cottoncandy""",
          version=__version__,
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
          install_requires=['six', 'botocore', 'boto3', 'python-dateutil',
                            'PyDrive'],
          **kwargs)

if __name__ == "__main__":
    main(**extra_setuptools_args)
