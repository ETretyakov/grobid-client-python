#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='grobid-client-python-pep',
      version='0.0.1',
      description='grobid-client-python-pep',
      author='kermitt2+ETretyakov',
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      license='LICENSE',
    )
