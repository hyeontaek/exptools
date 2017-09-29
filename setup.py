'''A setup module for exptools.'''

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='exptools',

    version='0.1.0',

    description='Experiment tools',
    long_description='Experiment tools for system projects',

    url='https://github.com/hyeontaek/exptools',

    # Author details
    author='Hyeontaek Lim',
    author_email='hl@cs.cmu.edu',

    # Choose your license
    license='Apache',

    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',

        'Topic :: Software Development',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 3.6',
    ],

    keywords='experiment',

    packages=find_packages(),

    install_requires=[
      'pytz',
      'tzlocal',
      'termcolor',
      'pandas',
      ],

    extras_require={
        'dev': [
          'wheel',
          'pylint',
          ],
        'sample': [
          'jupyter',
          'matplotlib',
          ],
    },

    package_data={},

    data_files=[],

    entry_points={},
)
