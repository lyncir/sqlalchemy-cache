# -*- coding: utf-8 -*-

from hashlib import md5
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
            cached_value = cache.get(cache_key,
                                     expiration_time=expiration_time,
                                     ignore_expiration=ignore_expiration)
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

    def key_from_query(sel, qualifier=None):
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

        values = [str(comliled)]
        for k in sorted(params):
            values.append(repr(params[k]))
        key = u" ".join(values)
        return md5(key.encode('utf8')).hexdigest()


class FromCache(MapperOption):

    propagate_to_loaders = False

    def __init__(self, cache, cache_key=None):
        self.cache = cache 
        self.cache_key = cache_key

    def process_query(self, query):
        query._cache = self

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('cache', None)
        return d
