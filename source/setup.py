# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

import wtables

VERSION = wtables.__version__
NAME = 'wtables'
DESCRIPTION = 'Web of Data Tables Project'

with open('README.md') as f:
    LONG_DESCRIPTION = f.read()

AUTHOR = 'emir@emunoz.org'
URL = 'http://emunoz.org'

with open('LICENSE') as f:
    LICENSE = f.read()

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author=AUTHOR,
    url=URL,
    license=LICENSE,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'beautifulsoup4>=4.6.0',
        'scikit-learn>=0.18.1',
        'numpy>=1.13.0',
        'scipy',
        'bidict',
        'pandas',
        'tqdm',
        'requests',
        'smart_open>=1.5.7',
        'pytest>=3.2.0',
        'joblib>=0.10.3',
        'bidict>=0.12.0',
        'sphinx>=1.3',
        'guzzle_sphinx_theme>=0.7.11','strsim',
        'Whoosh',
        'nltk',
        'torch'
    ],
    extras_require={
        'tf': ['tensorflow>=1.0.1'],
        'tf_gpu': ['tensorflow-gpu>=1.0.1']
    }
)
