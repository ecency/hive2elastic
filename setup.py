# coding=utf-8
import sys

from setuptools import find_packages
from setuptools import setup

assert sys.version_info[0] == 3 and sys.version_info[1] >= 5, "hive2elastic requires Python 3.5 or newer"

setup(
    name='hive2elastic',
    version='0.0.1',
    description='hive to elastic exporter',
    long_description=open('README.md').read(),
    packages=find_packages(),
    install_requires=[
        'configargparse',
        'elasticsearch',
        'sqlalchemy',
        'psycopg2-binary',
        'mistune'
    ],
    entry_points={
        'console_scripts': [
            'hive2elastic=main:main',
        ]
    })
