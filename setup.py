#!/usr/bin/python
from setuptools import find_packages
from setuptools import setup
setup(
    name='my-project-practice',
    version='1.0',
    install_requires=[
            'apache-beam[gcp]==2.50.0',
            'geopy==2.4.1',
            'geographiclib==2.0',
            'apache-beam==2.41.0',
            'google-api-core==2.11.0',
            'google-cloud-bigquery-storage==2.13.2',
            'google-cloud-storage==2.7.0',
            'google-cloud-bigquery==2.34.4'
    ],
    packages=find_packages(exclude=['notebooks']),
    py_modules=['config'],
    include_package_data=True,
    description='Coding Challenge'
)