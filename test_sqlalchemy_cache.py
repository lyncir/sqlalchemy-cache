# -*- coding: utf-8 -*-

import time
import pytest
from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.orm import relationship, joinedload
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy_cache import FromCache, create_scoped_session, CacheableMixin, \
        RelationshipCache


Base = declarative_base()
engine = create_engine('mysql://root@localhost/test', isolation_level="SERIALIZABLE")
session = create_scoped_session(engine, autocommit=False)


def _get_timestamp(self):
    return time.time()


class User(Base, CacheableMixin):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    desc = Column(String(200))
    count = Column(Integer)
    created_at = Column(DateTime, default=_get_timestamp)


class Address(Base, CacheableMixin):
    __tablename__ = 'addresses'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    email = Column(String(50))

    user = relationship("User", back_populates="addresses")

User.addresses = relationship(
        "Address", order_by=Address.id, back_populates="user")


@pytest.fixture
def init_db():
    User.__table__.create(engine)
    user = User(id=1, name="root", desc="test", count=500, created_at=_get_timestamp)
    session.add(user)
    session.commit()


def test_sqlalchemy_cache(init_db):

    # query
    query = session.query(User).filter_by(name='root')
    cache_query = query.options(FromCache(User.cache))
    
    # no cache
    name = query.one().name
    assert name == "root"

    # cache
    cache_name = cache_query.one().name
    assert cache_name == "root"

def relationship_cache_example():
    rc = RelationshipCache(Address.user, Address.cache)
    q = session.query(User).options(joinedload(User.addresses), rc)
    for i in q:
        print i.name
