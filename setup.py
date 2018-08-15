#!/usr/bin/env python
import sys

try:
    import configparser
except ImportError:
    import ConfigParser as configparser


if len(set(('develop', 'bdist_wheel', 'bdist_egg', 'bdist_rpm', 'bdist', 'bdist_dumb',
            'bdist_wininst', 'install_egg_info', 'egg_info', 'easy_install',
            )).intersection(sys.argv)) > 0:
    from setuptools import setup
else:
    # Use standard
    from distutils.core import setup

from distutils.command.install import install

def set_default_options(optfile):
    config = configparser.ConfigParser()
    config.read(optfile)
    with open(optfile, 'w') as fp:
        config.write(fp)

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
          version='0.2.0',
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
          long_description = long_description,
          **kwargs)

if __name__ == "__main__":
    main(**extra_setuptools_args)
