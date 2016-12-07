# -*- coding: utf-8 -*-

import time
import pytest
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy_cache import FromCache, Cache, create_scoped_session


Base = declarative_base()
engine = create_engine('mysql://root@localhost/test', isolation_level="SERIALIZABLE")
session = create_scoped_session(engine)
cache = Cache()


def _get_timestamp(self):
    return time.time()


class User(Base):
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    desc = Column(String(200))
    count = Column(Integer)
    created_at = Column(DateTime, default=_get_timestamp)


@pytest.fixture
def init_db():
    User.__table__.create(engine)
    user = User(id=1, name="root", desc="test", count=0, created_at=_get_timestamp)
    session.add(user)
    session.commit()


def test_sqlalchemy_cache(init_db):

    # query
    query = session.query(User).filter_by(name='root')
    cache_query = query.options(FromCache(cache))
    
    # no cache
    name = query.one().name
    assert name == "root"

    # cache
    cache_name = cache_query.one().name
    assert cache_name == "root"

    # invalidate cache 
    cache_query.invalidate()
    key = cache_query.key_from_query()
    assert cache.get(key) == None
