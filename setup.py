#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name = 'pg_gcs_backup',
    version = '0.1',
    packages = find_packages(),
    url = 'https://gitlab.com/tuankiet65/pg_gcs_backup',
    license = '',
    author = 'Ho Tuan Kiet',
    author_email = '',
    description = '',
    entry_points = {
        "console_scripts": [
            "pg_gcs_backup=pg_gcs_backup.__main__:main"
        ]
    }
)
