# -*- coding: utf-8 -*-

import time
import redis
from werkzeug.contrib.cache import RedisCache
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy_cache import FromCache, CachingQuery


Base = declarative_base()
regions = {}
Session = scoped_session(
                sessionmaker(
                    query_cls=query_callable(regions)
                )
            )
engine = create_engine('mysql://lyncir:ccm_86955516@192.168.1.200/test')
Session.configure(bind=engine)()
cache = RedisCache()


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
    user = Session.query(User).options(FromCache()).filter_by(name='root').one()
    print user.__dict__


def main():
    #Base.metadata.create_all(engine)
    test_cache()


if __name__ == '__main__':
    main()
