# -*- coding: utf-8 -*-

import pickle
from hashlib import md5
from contextlib import contextmanager
from redis import StrictRedis
from sqlalchemy.orm.interfaces import MapperOption
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

    def invalidate(self):
        """Invalidate the cache value represented by this Query."""
        cache, cache_key = self._get_cache_plus_key()
        cache.delete(cache_key)

    def get_value(self, merge=True, createfunc=None,
                  expiration_time=None, ignore_expiration=False):
        """
        Return the value from the cache for this query.
        """
        cache, cache_key = self._get_cache_plus_key()

        assert not ignore_expiration or not createfunc, \
            "Can't ignore expiration and also provide createfunc"

        if ignore_expiration or not createfunc:
            cached_value = cache.get(cache_key)
        else:
            cached_value = cache.get(cache_key)
            if not cached_value:
                cached_value = createfunc()
                cache.set(cache_key, cached_value, timeout=expiration_time)

        if cached_value and merge:
            cached_value = self.merge_result(cached_value, load=False)

        return cached_value

    def set_value(self, value):
        """Set the value in the cache for this query."""
        cache, cache_key = self._get_cache_plus_key()
        cache.set(cache_key, value)

    def update_value(self, query):
        cache, cache_key = self._get_cache_plus_key()
        createfunc = lambda: list(query.__iter__())
        value = createfunc()
        cache.set(cache_key, value)

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
            default_timeout=300, **kwargs):
        self.default_timeout = default_timeout
        self.cache = StrictRedis(host, port, db, password, **kwargs)

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

    def get(self, key):
        key_str = self.cache.get(key)
        if key_str is None:
            return None
        key_l = key_str.split(",")
        value = []
        for v_key in key_l:
            v = self.load_object(self.cache.get(name=v_key))
            value.append(v)
        return value

    def set(self, key, value, timeout=None):
        timeout = self._normalize_timeout(timeout)
        if isinstance(value, list):
            key_l = []
            for v in value:
                v_key = ":".join([v.__tablename__, str(v.id)])
                dump = self.dump_object(v)
                if timeout == -1:
                    res = self.cache.set(name=v_key, value=dump)
                else:
                    res = self.cache.setex(name=v_key, value=dump, time=timeout)
                key_l.append(v_key)

            key_str = ",".join(key_l)
            if timeout == -1:
                result = self.cache.set(name=key, value=key_str)
            else:
                result = self.cache.setex(name=key, value=key_str, time=timeout)
            return result

    def add(self, key, value, timeout=None):
        timeout = self._normalize_timeout(timeout)
        dump = self.dump_object(value)
        return (
            self.cache.setnx(name=key, value=dump) and
            self.cache.expire(name=key, time=timeout)
        )

    def delete(self, key):
        return self.cache.delete(key)

    def blpop(self, *args, **kwargs):
        return self.cache.blpop(*args, **kwargs)

    def rpush(self, *args, **kwargs):
        return self.cache.rpush(*args, **kwargs)

    def lpush(self, *args, **kwargs):
        return self.cache.lpush(*args, **kwargs)

    def getset(self, *args, **kwargs):
        return self.cache.getset(*args, **kwargs)


class Lock(object):
    """Lock implemented on top of redis."""

    def __init__(self, client, name, timeout=60, db=0):
        """
        Create, if necessary the lock variable in redis.
        We utilize the ``blpop`` command and its blocking behavior.
        The ``_key`` variable is used to check, whether the mutex exists or not,
        while the ``_mutex`` variable is the actual mutex.
        """
        self._key = 'lock:name:%s' % name
        self._mutex = 'lock:mutex:%s' % name
        self._timeout = timeout
        self._r = client
        self._init_mutex()

    @property
    def mutex_key(self):
        return self._mutex

    def lock(self):
        """
        Lock and block.

         Raises:
             RuntimeError, in case of synchronization issues.
        """
        res = self._r.blpop(self._mutex, self._timeout)
        if res is None:
            raise RuntimeError

    def unlock(self):
        self._r.rpush(self._mutex, 1)

    def _init_mutex(self):
        """
        Initialize the mutex, if necessary.

        Use a separate key to check for the existence of the "mutex",
        so that we can utilize ``getset``, which is atomic.
        """
        exists = self._r.getset(self._key, 1)
        if exists is None:
            self._r.lpush(self._mutex, 1)


@contextmanager
def lock(client, name, timeout=60):
    """Lock on name using redis."""
    l = Lock(client, name, timeout)
    try:
        l.lock()
        yield
    finally:
        l.unlock()
