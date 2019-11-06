# Overview

Projects such as Django and Flask ship with what is known as ORM (object
relational mappers). These abstractions expose much of the underlying
relational behavior (both in schema and queries).This project on the
other hand allows a programmer to stay entirely in the object domain
(hiding any relational functionality contained within), while still
allowing transparent mapping to a relational database. This transparent
mapping functionality is not included in this release.

# Installation

Requires python3.x

```
./setup.py build
./setup.py test
./setup.py install
```

# License

This project is made available under the Apache License, version 2.0.

See [LICENSE.txt](license.txt) for details.

