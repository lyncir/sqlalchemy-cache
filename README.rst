===================
SQLAlchemy Cache
===================

.. image:: https://travis-ci.org/lyncir/sqlalchemy-cache.svg?branch=master
  :target: https://travis-ci.org/lyncir/sqlalchemy-cache
  :alt: Build Status

Required
===================

- use redis
- user SQLAlchemy


Storage
===================

::
    key                 value
    sql(md5)            result(str). eg. table:id, ...
    table:id            sql value pickle

Usage
===================
1. don't support use sqlalchemy get by id.
