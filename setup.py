#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Author: Tim Henderson
#Email: tim.tadh@gmail.com
#For licensing see the LICENSE file in the top level directory.

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name = 'pybptree',
    version = 'git master',
    description = 'Go file-structures/bptree RPC interface',
    author = 'Tim Henderson',
    author_email = 'tim.tadh@gmail.com',
    url = 'http://github.com/timtadh/file-structures',
    keywords = ['b+tree'],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "License :: OSI Approved :: GPL",
        "Operating System :: POSIX",
        "Topic :: Utilities",
        "Intended Audience :: Developers",
        ],
    packages = find_packages(),
    include_package_data = False,
    zip_safe = False
)

