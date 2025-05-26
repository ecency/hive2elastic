# coding=utf-8
import sys

from setuptools import find_packages
from setuptools import setup

assert sys.version_info[0] == 3 and sys.version_info[1] >= 5, "hive2elastic requires Python 3.5 or newer"

setup(
    name='hive2elastic',
    version='0.0.6',
    description='hive to elastic exporter',
    long_description=open('README.md').read(),
    packages=find_packages(),
    install_requires=[
        'configargparse==1.4.1',
        'elasticsearch==7.15.1',
        'sqlalchemy==1.4.15',
        'psycopg2-binary==2.8.6',
        'markdown2==2.4.1',
        'timeout_decorator==0.5.0'
    ],
    entry_points={
        'console_scripts': [
            'hive2elastic_post=post.indexer:main'
        ]
    })
