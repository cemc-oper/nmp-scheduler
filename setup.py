# coding=utf-8
from setuptools import setup, find_packages

setup(
    name='nmp-scheduler',

    version='4.0',

    description='The scheduler for NMP by NWPC.',
    long_description=__doc__,

    packages=find_packages(exclude=['conf', 'tests']),

    include_package_data=True,

    zip_safe=False,

    install_requires=[
        'click',
        'SQLAlchemy',
        'mysql-connector-python',
        'pymongo',
        'mongoengine',
        'PyYAML',
        'redis',
        'celery>=4.0,<4.2',
        'fabric',
        'nwpc_workflow_model',
        'grpcio',
        'googleapis-common-protos',
    ],

    extras_require={
        'testing': [
            'pytest',
            'mongomock'
        ]
    }
)
