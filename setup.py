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
    version="0.4",
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
    test_requires=["pytest"],
    extras_require={
        "fquery": ["aioitertools"],
        "pydantic": ["pydantic"],
        "sql": ["pypika >= 0.36.5"],
        "sqlmodel": [
            "sqlmodel@git+https://github.com/adsharma/sqlmodel.git@sqlmodel_rebuild",
            "duckdb_engine >= 0.14.0",
            "inflection >= 0.5.1",
            "sqlalchemy >= 2.0.36",
        ],
        "graphql": ["strawberry-graphql >= 0.37.1"],
        "polars": ["polars >= 0.12.0"],
        "pyarrow": ["pyarrow"],
        "django": ["django"],
        "malloy": [],
        "cypher": [],
        "ladybug": ["ladybug; python_version >= '3.10'"],
        "df": ["polars >= 0.12.0"],
    },
)
