#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fquery",
    version="0.2",
    description="A graph query engine",
    url="https://github.com/adsharma/fquery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    test_suite="tests",
    install_requires=["aioitertools"],
    test_requires=["sqlalchemy >= 2.0.36"],
    extras_require={
        "SQL": ["pypika >= 0.36.5", "sqlmodel >= 0.0.22", "duckdb_engine >= 0.14.0"],
        "graphql": ["strawberry >= 0.37.1"],
    },
)
