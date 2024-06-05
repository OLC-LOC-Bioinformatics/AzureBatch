#!/usr/bin/env python3
"""
Set up the package
"""

# Standard imports
import importlib.util
import os

# Third party imports
from setuptools import setup, find_packages

__author__ = 'adamkoziol'

# Find the version
version_file_path = os.path.join('azure_batch', 'version.py')
spec = importlib.util.spec_from_file_location("version", version_file_path)
version_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(version_module)
version = version_module.__dict__

# Read the contents of the README file
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

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
    long_description=long_description,
    # The content type of the long description. Necessary for PyPI
    long_description_content_type='text/markdown',
    # Classifiers categorize the project for users.
    classifiers=[
        # Specifies the intended audience of the project
        'Intended Audience :: Science/Research',
        # Defines the license of the project
        'License :: OSI Approved :: MIT License',
        # Specifies the supported Python versions
        'Programming Language :: Python :: 3.12',
        # Indicates the development status of the project
        'Development Status :: 5 - Production/Stable',
        # Specifies the topic related to the project
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
