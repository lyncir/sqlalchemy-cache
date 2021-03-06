# -*- coding: utf-8 -*-

import time
import uuid
import pickle
from hashlib import md5
from redis import StrictRedis
from sqlalchemy import event
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.orm.attributes import get_history
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm.query import Query


class CachingQuery(Query):
    """
    A Query subclass which optionally loads full results from cache.

    The CachingQuery optionally stores additional state that allows it to
    consult a cache before accessing the database, in the form of a FromCache
    or RelationshipCache object.
    Each of these objects refer to a cache. When such an object has associated
    itself with the CachingQuery, it is used to locate a cached result.
    If none is present, then the Query is invoked normally, the results
    being cached.
    The FromCache and RelationshipCache mapper options below represent
    the "public" method of configuring this state upon the CachingQuery.
    """

    def __iter__(self):
        """
        Override __iter__ to pull results from cache if particular
        attributes have been configured.
        This approach does *not* detach the loaded objects from the current
        session. If the cache backend is an in-process cache (like "memory")
        and lives beyond the scope of the current session's transaction, those
        objects may be expired.
        The method here can be modified to first expunge() each loaded item
        from the current session before returning the list of items, so that
        the items in the cache are not the same ones in the current Session.
        """
        if hasattr(self, '_cache'):
            func = lambda: list(Query.__iter__(self))
            return iter(self.get_value(createfunc=func))
        else:
            return Query.__iter__(self)

    def _get_cache_plus_key(self):
        """Return a cache region plus key."""
        key = getattr(self, '_cache_key', self.key_from_query())
        return self._cache.cache, key

    def get_value(self, merge=True, createfunc=None,
                  expiration_time=None, ignore_expiration=False):
        """
        Return the value from the cache for this query.
        """
        cache, cache_key = self._get_cache_plus_key()

        assert not ignore_expiration or not createfunc, \
            "Can't ignore expiration and also provide createfunc"

        if ignore_expiration or not createfunc:
            cached_value = cache._get(cache_key)
        else:
            cached_value = cache._get(cache_key)
            if not cached_value:
                cached_value = createfunc()
                cache._set(cache_key, cached_value, timeout=expiration_time)

        #if cached_value and merge:
        #    cached_value = self.merge_result(cached_value, load=False)

        return cached_value

    def key_from_query(self, qualifier=None):
        """
        Given a Query, create a cache key.
        There are many approaches to this; here we use the simplest, which is
        to create an md5 hash of the text of the SQL statement, combined with
        stringified versions of all the bound parameters within it.
        There's a bit of a performance hit with compiling out "query.statement"
        here; other approaches include setting up an explicit cache key with a
        particular Query, then combining that with the bound parameter values.
        """
        stmt = self.with_labels().statement
        compiled = stmt.compile()
        params = compiled.params

        values = [str(compiled)]
        for k in sorted(params):
            values.append(repr(params[k]))
        key = u" ".join(values)
        return md5(key.encode('utf8')).hexdigest()


class _CacheableMapperOption(MapperOption):

    def __init__(self, cache, cache_key=None):
        """
        Construct a new `_CacheableMapperOption`.
        :param cache: the cache.  Should be a StrictRedis instance.
        :param cache_key: optional.  A string cache key that will serve as
        the key to the query. Use this if your query has a huge amount of
        parameters (such as when using in_()) which correspond more simply to
        some other identifier.
        """
        self.cache = cache
        self.cache_key = cache_key


class FromCache(_CacheableMapperOption):
    """Specifies that a Query should load results from a cache."""

    propagate_to_loaders = False

    def process_query(self, query):
        """Process a Query during normal loading operation."""
        query._cache = self


class RelationshipCache(_CacheableMapperOption):
    """
    Specifies that a Query as called within a "lazy load" should load
    results from a cache.
    """

    propagate_to_loaders = True

    def __init__(self, attribute, cache, cache_key=None):
        """
        Construct a new RelationshipCache.
        :param attribute: A Class.attribute which indicates a particular
        class relationship() whose lazy loader should be pulled from the cache.
        :param cache_key: optional.  A string cache key that will serve as the
        key to the query, bypassing the usual means of forming a key from the
        Query itself.
        """
        super(RelationshipCache, self).__init__(cache, cache_key)
        self._relationship_options = {
            (attribute.property.parent.class_, attribute.property.key): self
        }

    def process_query_conditionally(self, query):
        """
        Process a Query that is used within a lazy loader.
        (the process_query_conditionally() method is a SQLAlchemy
        hook invoked only within lazyload.)
        """
        if query._current_path:
            mapper, prop = query._current_path[-2:]
            for cls in mapper.class_.__mro__:
                k = (cls, prop.key)
                relationship_option = self._relationship_options.get(k)
                if relationship_option:
                    query._cache = relationship_option
                    break

    def and_(self, option):
        """
        Chain another RelationshipCache option to this one.
        While many RelationshipCache objects can be specified on a single
        Query separately, chaining them together allows for a more efficient
        lookup during load.
        """
        self._relationship_options.update(option._relationship_options)
        return self


class Cache(object):

    def __init__(self, host='localhost', port=6379, db=0, password=None,
            default_timeout=300, model=None, key_prefix='query', **kwargs):
        self.model = model
        self.default_timeout = default_timeout
        self.cache = StrictRedis(host, port, db, password, **kwargs)
        # 自定义主键或默认id
        self.pk = getattr(model, 'cache_pk', 'id')
        self.key_prefix = key_prefix

    def _normalize_timeout(self, timeout):
        if timeout is None:
            timeout = self.default_timeout
        if timeout == 0:
            timeout = -1
        return timeout

    def row2dict(self, row):
        d = {}
        for column in row.__table__.columns:
            d[column.name] = getattr(row, column.name)
        return d

    def dump_object(self, value):
        t = type(value)
        if t in (int, long):
            return str(value).encode('ascii')
        return b'!' + pickle.dumps(value)

    def load_object(self, value):
        if value is None:
            return None
        if value.startswith(b'!'):
            try:
                return pickle.loads(value[1:])
            except pickle.PickleError:
                return None
        try:
            return int(value)
        except ValueError:
            return value

    def _set_setex(self, key, value, timeout=None):
        """
        set or setex with timeout
        """
        timeout = self._normalize_timeout(timeout)
        if timeout == -1:
            result = self.cache.set(name=key, value=value)
        else:
            result = self.cache.setex(name=key, value=value, time=timeout)
        return result

    def _set_row(self, value, timeout=None):
        """
        set row with dump value
        """
        timeout = self._normalize_timeout(timeout)
        v_key = ":".join([self.model.__tablename__, self.pk, str(getattr(value, self.pk))])
        v_value = self.dump_object(value)
        self._set_setex(v_key, v_value, timeout)
        return v_key

    def _get(self, key):
        new_key = ":".join([self.key_prefix, self.model.__tablename__, key])
        key_str = self.cache.get(new_key)
        if key_str is None:
            return None
        key_l = key_str.split(",")
        value = []
        for v_key in key_l:
            v = self.load_object(self.cache.get(name=v_key))
            value.append(v)
        return value

    def _set(self, key, value, timeout=None):
        timeout = self._normalize_timeout(timeout)
        if isinstance(value, list):
            key_l = []
            for v in value:
                v_key = self._set_row(v) 
                key_l.append(v_key)

            key_str = ",".join(key_l)
            new_key = ":".join([self.key_prefix, self.model.__tablename__, key])
            self._set_setex(key=new_key, value=key_str)

    def set(self, *args, **kwargs):
        return self.cache.set(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self.cache.get(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.cache.delete(*args, **kwargs)

    def blpop(self, *args, **kwargs):
        return self.cache.blpop(*args, **kwargs)

    def rpush(self, *args, **kwargs):
        return self.cache.rpush(*args, **kwargs)

    def lpush(self, *args, **kwargs):
        return self.cache.lpush(*args, **kwargs)

    def getset(self, *args, **kwargs):
        return self.cache.getset(*args, **kwargs)

    def exists(self, *args, **kwargs):
        return self.cache.exists(*args, **kwargs)

    def hset(self, *args, **kwargs):
        return self.cache.hset(*args, **kwargs)

    def pexpire(self, *args, **kwargs):
        return self.cache.pexpire(*args, **kwargs)

    def hexists(self, *args, **kwargs):
        return self.cache.hexists(*args, **kwargs)

    def hincrby(self, *args, **kwargs):
        return self.cache.hincrby(*args, **kwargs)

    def pttl(self, *args, **kwargs):
        return self.cache.pttl(*args, **kwargs)

    def scan_iter(self, *args, **kwargs):
        return self.cache.scan_iter(*args, **kwargs)

    def _columns(self):
        return [c.name for c in self.model.__table__.columns if c.name == self.pk]

    def _delete(self, obj):
        """
        sqlalchemy delete event 
        1. 清除row缓存 table:id:1
        2. 清除query缓存 query:table:md5
        """
        for column in self._columns():
             added, unchanged, deleted = get_history(obj, column)
             for value in list(deleted) + list(added) + list(unchanged):
                 key = ":".join([self.model.__tablename__, column, str(value)])
                 self.delete(key)

        query_key = ":".join([self.key_prefix, self.model.__tablename__, "*"])
        for key in self.scan_iter(query_key):
            self.delete(key)

    def _insert(self, obj):
        """
        sqlalchemy insert event 
        1. 清除query缓存 query:table:md5
        """
        query_key = ":".join([self.key_prefix, self.model.__tablename__, "*"])
        for key in self.scan_iter(query_key):
            self.delete(key)

    def _update(self, obj):
        """
        sqlalchemy update event
        1. 更新row缓存 table.id.1
        """
        key = ":".join([self.model.__tablename__, self.pk, str(getattr(obj, self.pk))])
        if self.exists(key):
            self._set_row(obj)


class Lock(object):
    """Lock implemented on top of redis."""

    def __init__(self, client, name, timeout=0.1, db=0):
        """
        Create, if necessary the lock variable in redis.

        :param client: redis client
        :param name: redis key
        :param timeout: second
        :param db: redis db
        """
        self._key = 'lock:name:%s' % name
        self._timeout = int(timeout * 1000)
        self._r = client
        self._uuid4 = uuid.uuid4()

    def acquire(self):
        """
        Acquire

        Return: None -> get a lock. long -> have a lock, get time
        """
        # 检查是否key已经被占用，如果没有则设置超时时间和唯一标识，初始化value=1
        if self._r.exists(self._key) == 0:
            self._r.hset(self._key, self._uuid4, 1)
            self._r.pexpire(self._key, self._timeout)
            return None
        # 如果锁重入,需要判断锁的key field 都一致情况下 value 加1
        if self._r.hexists(self._key, self._uuid4) == 1:
            self._r.hincrby(self._key, self._uuid4, 1)
            self._r.pexpire(self._key, self._timeout)
            return None
        return self._r.pttl(self._key)

    def lock(self):
        # 申请锁，返回还剩余的锁过期时间
        ttl = self.acquire()
        # 如果为空，表示申请锁成功
        if ttl is None:
            return True

        while True:
            # 再次尝试一次申请锁
            ttl = self.acquire()
            # 获得锁，返回
            if ttl is None:
                return True

            # 等待锁
            if ttl >= 0:
                sec = ttl / 1000.0
                time.sleep(sec)
            else:
                return False

    def unlock(self):
        """
        Unlock

        Return: 1 -> unlock a key, None -> unlock failed
        """
        # 如果key已经不存在，说明已经被解锁
        if self._r.exists(self._key) == 0:
            return 1
        # key和field不匹配，说明当前客户端线程没有持有锁，不能主动解锁。
        if self._r.hexists(self._key, self._uuid4) == 0:
            return None
        lock_count = self._r.hincrby(self._key, self._uuid4, -1)
        # 如果counter>0说明锁在重入，不能删除key
        if lock_count > 0:
            self._r.pexpire(self._key, self._timeout)
            return 0
        else:
            self._r.delete(self._key)
            return 1
        return None


class CacheableMixin(object):

    @declared_attr
    def cache(cls):
        return Cache(model=cls)

    @staticmethod
    def _delete_event(mapper, connection, target):
        target.cache._delete(target)

    @staticmethod
    def _insert_event(mapper, connection, target):
        target.cache._insert(target)

    @staticmethod
    def _update_event(mapper, connection, target):
        target.cache._update(target)

    @classmethod
    def __declare_last__(cls):
        """
        监听增，删，改事件，并进行相应的缓存更新
        """
        event.listen(cls, 'before_insert', cls._insert_event)
        event.listen(cls, 'before_delete', cls._delete_event)
        event.listen(cls, 'before_update', cls._update_event)
