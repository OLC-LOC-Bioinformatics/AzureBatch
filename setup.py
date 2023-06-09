#!/usr/bin/env python
from setuptools import setup, find_packages
from distutils.util import convert_path
import os
__author__ = 'adamkoziol'

# Find the version
version = dict()
with open(convert_path(os.path.join('azure_batch', 'version.py')), 'r') as version_file:
    exec(version_file.read(), version)
setup(
    name="AzureBatch",
    version=version['__version__'],
    entry_points={
        'console_scripts': [
            'AzureBatch = azure_batch.azure_cli:cli'
        ],
    },
    packages=find_packages(),
    include_package_data=True,
    author="Adam Koziol",
    author_email="adam.koziol@inspection.gc.ca",
    url="https://github.com/OLC-LOC-Bioinformatics/AzureBatch",
)
