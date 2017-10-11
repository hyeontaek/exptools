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
      'base58',
      'pytz',
      'tzlocal',
      'termcolor',
      'pandas',
      'requests',
      'json-rpc',
      'werkzeug',
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
          ],
    },

    package_data={
        '': ['sample/*.ipynb'],
        },

    data_files=[
        ('', ['LICENSE.txt']),
        ],

    entry_points={},
)
