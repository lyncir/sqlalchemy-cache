# -*- coding: utf-8 -*-

import re
import functools
import sqlalchemy
from sqlalchemy import orm, event, inspect
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import scoped_session, sessionmaker, Query


_camelcase_re = re.compile(r'([A-Z]+)(?=[a-z0-9])')


def _make_table(db):
    def _make_table(*args, **kwargs):
        if len(args) > 1 and isinstance(args[1], db.Column):
            args = (args[0], db.metadata) + args[1:]
        info = kwargs.pop('info', None) or {}
        info.setdefault('bind_key', None)
        kwargs['info'] = info
        return sqlalchemy.Table(*args, **kwargs)
    return _make_table


def _set_default_query_class(d, cls):
    if 'query_class' not in d:
        d['query_class'] = cls


def _wrap_with_default_query_class(fn, cls):
    @functools.wraps(fn)
    def newfn(*args, **kwargs):
        _set_default_query_class(kwargs, cls)
        if "backref" in kwargs:
            backref = kwargs['backref']
            if isinstance(backref, string_types):
                backref = (backref, {})
            _set_default_query_class(backref[1], cls)
        return fn(*args, **kwargs)
    return newfn


def _include_sqlalchemy(obj, cls):
    for module in sqlalchemy, sqlalchemy.orm:
        for key in module.__all__:
            if not hasattr(obj, key):
                setattr(obj, key, getattr(module, key))
    # Note: obj.Table does not attempt to be a SQLAlchemy Table class.
    obj.Table = _make_table(obj)
    obj.relationship = _wrap_with_default_query_class(obj.relationship, cls)
    obj.relation = _wrap_with_default_query_class(obj.relation, cls)
    obj.dynamic_loader = _wrap_with_default_query_class(obj.dynamic_loader, cls)
    obj.event = event


class _QueryProperty(object):
    def __init__(self, sa):
        self.sa = sa

    def __get__(self, obj, type):
        try:
            mapper = orm.class_mapper(type)
            if mapper:
                return type.query_class(mapper, session=self.sa.session())
        except UnmappedClassError:
            return None


def _should_set_tablename(bases, d):
    if '__tablename__' in d or '__table__' in d or '__abstract__' in d:
        return False

    if any(v.primary_key for v in d.itervalues() if isinstance(v, sqlalchemy.Column)):
        return True

    for base in bases:
        if hasattr(base, '__tablename__') or hasattr(base, '__table__'):
            return False

        for name in dir(base):
            attr = getattr(base, name)

            if isinstance(attr, sqlalchemy.Column) and attr.primary_key:
                return True


class _BoundDeclarativeMeta(DeclarativeMeta):

    def __new__(cls, name, bases, d):
        if _should_set_tablename(bases, d):
            def _join(match):
                word = match.group()
                if len(word) > 1:
                    return ('_%s_%s' % (word[:-1], word[-1])).lower()
                return '_' + word.lower()
            d['__tablename__'] = _camelcase_re.sub(_join, name).lstrip('_')

        return DeclarativeMeta.__new__(cls, name, bases, d)

    def __init__(self, name, bases, d):
        bind_key = d.pop('__bind_key__', None) or getattr(self, '__bind_key__', None)
        DeclarativeMeta.__init__(self, name, bases, d)
        if bind_key is not None and hasattr(self, '__table__'):
            self.__table__.info['bind_key'] = bind_key


class Model(object):
    query_class = None
    query = None


class SQLAlchemy(object):

    Query = None

    def __init__(self, engine, option=None, autocommit=False, model_class=Model,
            metadata=None, query_class=Query, options=None):
        self.Query = query_class
        self.session = self.create_scoped_session(engine, options, autocommit)
        self.Model = self.make_declarative_base(model_class, metadata)
        _include_sqlalchemy(self, query_class)

    def create_scoped_session(self, engine, options=None, autocommit=False):
        """Create a :class:`~sqlalchemy.orm.scoping.scoped_session`"""
        if options is None:
            options = {}

        options.setdefault('query_cls', self.Query)
        session = scoped_session(
                        sessionmaker(**options)
                    )
        session.configure(bind=engine, autocommit=autocommit)
        return session

    def make_declarative_base(self, model, metadata=None):
        base = declarative_base(cls=model, name='Model',
                                metadata=metadata,
                                metaclass=_BoundDeclarativeMeta)
        if not getattr(base, 'query_class', None):
            base.query_class = self.Query

        base.query = _QueryProperty(self)
        return base
