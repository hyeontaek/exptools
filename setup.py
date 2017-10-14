'''A setup.py for exptools.'''

from setuptools import setup, find_packages
from os import path

setup(
    name='exptools',

    version='0.1.0',

    description='Experiment tools',
    long_description='Experiment tools for system projects',

    url='https://github.com/hyeontaek/exptools',

    author='Hyeontaek Lim',
    author_email='hl@cs.cmu.edu',

    license='Apache',

    classifiers=[
      'Development Status :: 3 - Alpha',

      'Intended Audience :: Developers',
      'Intended Audience :: Science/Research',

      'Topic :: Scientific/Engineering',
      'Topic :: Software Development',

      'License :: OSI Approved :: Apache Software License',

      'Programming Language :: Python :: 3.6',
      ],

    keywords='experiment research',

    packages=find_packages(),

    install_requires=[
      'aiofiles',
      'base58',
      'pytz',
      'termcolor',
      'tzlocal',
      'websockets',
      'yaql',
      ],

    extras_require={
      'dev': [
        'wheel',
        'pylint',
        'pytest',
        'pytest-cov',
        ],
      'sample': [
        'jupyter',
        'matplotlib',
        'pandas',
        ],
      },

    package_data={},

    scripts=[
      'bin/exptools-server',
      'bin/exptools-client',
      'bin/etc',
      ],

    data_files=[
      ('', ['LICENSE.txt']),
      ],

    entry_points={},
)
