#! /usr/bin/env python

from setuptools import setup

setup(
    name='yadageengine',
    version='0.0.1',
    description='Web API for Yadage workflow engine',
    keywords='workflows reproducibility ',
    author='Heiko Mueller',
    author_email='heiko.muller@gmail.com',
    url='https://github.com/heikomuller/yadage-engine-api',
    license='GPLv3',
    packages=['yadageengine'],
    package_data={'': ['LICENSE']},
    install_requires=[
        'flask>=0.10',
        'flask-cors>=3.0.2',
        'pymongo',
        'celery',
        'redis',
        'yadage>=0.10.8'
    ]
)
