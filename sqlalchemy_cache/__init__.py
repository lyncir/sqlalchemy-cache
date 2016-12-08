# -*- coding: utf-8 -*-

from sqlalchemy.orm import scoped_session, sessionmaker

from .core import FromCache, CachingQuery, Cache, Lock


def create_scoped_session(engine, options=None, autocommit=False):
    """Create a :class:`~sqlalchemy.orm.scoping.scoped_session`"""
    if options is None:
        options = {}

    options.setdefault('query_cls', CachingQuery)
    session = scoped_session(
                    sessionmaker(**options)
                )
    session.configure(bind=engine, autocommit=autocommit)
    return session
