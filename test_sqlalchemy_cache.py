# -*- coding: utf-8 -*-

import time
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


def test_cache():
    query = session.query(User).filter_by(name='root')
    cache_query = query.options(FromCache(cache))
    
    # no cache
    print query.one().count

    # cache
    print cache_query.one().count

    # invalidate cache 
    print cache_query.invalidate()


def main():
    #Base.metadata.create_all(engine)
    test_cache()


if __name__ == '__main__':
    main()
