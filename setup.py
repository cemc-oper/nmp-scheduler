# coding=utf-8
from setuptools import setup

setup(
    name='nmp-scheduler',

    version='4.0',

    description='The scheduler for NMP by NWPC.',
    long_description=__doc__,

    packages=['nmp_scheduler'],

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
        'celery',
        'fabric',
        'nwpc_workflow_model'
    ],

    extras_require={
        'testing': [
            'pytest',
            'mongomock'
        ]
    }
)
