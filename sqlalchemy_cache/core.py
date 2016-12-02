# -*- coding: utf-8 -*-

from werkzeug.contrib.cache import RedisCache
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.orm.query import Query


class CachingQuery(Query):
    pass


class FromCache(MapperOption):
    pass
