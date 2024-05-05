# Overview

Projects such as Django and Flask ship with what is known as ORM (object
relational mappers). These abstractions expose much of the underlying
relational behavior (both in schema and queries).This project on the
other hand allows a programmer to stay entirely in the object domain
(hiding any relational functionality contained within), while still
allowing transparent mapping to a relational database.

Only basic transparent mapping of fqueries to SQL is supported:
[Demo](https://github.com/adsharma/fquery/blob/main/tests/test_sql.py).

Only basic transparent mapping of fqueries to [malloy](https://www.malloydata.dev/) is supported:
[Demo](https://github.com/adsharma/fquery/blob/main/tests/test_malloy.py).

# Installation

Requires python3.x

```
pip3 install fquery
```

Running tests:

```
alias t=pytest-3
t
```

You can also run it via tox.

# Tutorial

[Intro](https://adsharma.github.io/fquery/): What is fquery, sample queries
and some information on internals.

[Blog post](https://adsharma.github.io/django-fquery/) on how to use
fquery with Django and get easy access to graphql functionality


# License

This project is made available under the Apache License, version 2.0.

See [LICENSE.txt](license.txt) for details.

