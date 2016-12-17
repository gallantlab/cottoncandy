#!/usr/bin/env python
import sys

if len(set(('develop', 'bdist_egg', 'bdist_rpm', 'bdist', 'bdist_dumb',
            'bdist_wininst', 'install_egg_info', 'egg_info', 'easy_install',
            )).intersection(sys.argv)) > 0:
    from setuptools import setup
else:
    # Use standard
    from distutils.core import setup

if not 'extra_setuptools_args' in globals():
    extra_setuptools_args = dict()

long_description = """
"""

def main(**kwargs):
    setup(name="""cottoncandy""",
          version='0.01',
          description="""sugar for S3""",
          author='Anwar O. Nunez-Elizalde',
          author_email='anwarnunez@gmail.com',
          url='gallantlab.github.io/cottoncandy/',
          packages=['cottoncandy',
                    ],
          long_description = long_description,
          **kwargs)

if __name__ == "__main__":
    main(**extra_setuptools_args)
