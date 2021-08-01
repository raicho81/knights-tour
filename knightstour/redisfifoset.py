import cachetools.func
import logging
import redis
from knightstour import FIFOSet


class RedisFIFOSet:
    """
        Class representing a set, which can be bound in size. When the maximum size is reached the first
        elements added equal to evict_count are removed to make place for new ones or if the elements are less than
        evict_count then only the available are evicted resulting in an empty set.
        If maxsize is None the set behaves like an ordinary set() with the extra load of
        a FIFO list for the SET keys Stored in Redis.
    """

    def __init__(self, maxsize=None,
                evict_count=1000,
                redis_pool_obj=redis.Redis(),
                redis_set_key=None,
                redis_ev_list_key=None,
                redis_hits_key=None,
                redis_misses_key=None):

        self.__hits_key = redis_hits_key
        self.__misses_key = redis_misses_key
        self.__maxsize = maxsize
        self.__evict_count = evict_count
        self.__set_key = redis_set_key
        self.__set_evict_list_key = redis_ev_list_key
        self.__r = redis_pool_obj

    def clean_redis_structures(self):
        
        def trans_func_delete(p):
            p.delete(*[self.__set_evict_list_key, self.__set_key])
            p.set(self.__hits_key, 0)
            p.set(self.__misses_key, 0)

        self.__r.transaction(trans_func_delete, *[self.__set_evict_list_key, self.__set_key,])

    def __repr__(self):
        s = [key for key in self]

        def trans_func(p):
            return p.lrange(self.__set_evict_list_key, 0, -1)

        l = self.__r.transaction(trans_func, *[self.__set_evict_list_key])
        
        return "{} (set: {}, list: {}, maxsize={}, currsize={}, hits={}, misses={}, evict_count={})".format(
            self.__class__.__name__,
            s,
            l,
            self.__maxsize,
            self.currsize,
            self.hits,
            self.misses,
            self.__evict_count
        )

    @cachetools.func.lru_cache(maxsize=131112)
    def __contains__(self, key):

        def trans_func(p):
            ret = bool(p.sismember(self.__set_key, key))
        
        ret = self.__r.transaction(trans_func, *[self.__set_key])

        if ret:
            self.__r.incr(self.__hits_key)
        else:
            self.__r.incr(self.__misses_key)
            self.__evict(key)
            self.add(key)
        
        return ret

    def __iter__(self):
        # TODO: Do I need Transaction here? I am not sure yet.
        return  iter(self.__r.sscan_iter(self.__set_key))

    def __len__(self):
        # TODO: Do I need Transaction here? I am not sure yet.
        return self.__r.llen(self.__set_evict_list_key)
    
    def __evict(self, key):
        cursz = self.currsize
        
        if self.__maxsize and (cursz + self.getsizeof(key)) < self.__maxsize:
            return
        how_much_to_evict = min(self.__evict_count, cursz)
        
        def trans_func_to_evict(p):
            llen = p.llen(self.__set_evict_list_key)
            return p.lrange(self.__set_evict_list_key, llen - how_much_to_evict, llen)
    
        to_evict = self.__r.transaction(trans_func_to_evict, *[self.__set_evict_list_key])

        def trans_func_rpop(p):
            for _ in to_evict:
                p.rpop(self.__set_evict_list_key)

        self.__r.transaction(trans_func_rpop, *[self.__set_evict_list_key])

        def trans_func_srem(p):            
            p.srem(self.__set_key, elm_to_evict)
            self.__contains__.pop(key)

        self.__r.transaction(trans_func_srem, *[self.__set_key,])       

    def __add(self, key):
        def trans_func_sadd(p):
            p.sadd(self.__set_key, key)

        def trans_func_lpush(p):
            p.lpush(self.__set_evict_list_key, key)

        self.__r.transaction(trans_func_sadd, *[self.__set_key])
        self.__r.transaction(trans_func_lpush, *[self.__set_evict_list_key])

    def add(self, key):
        self.__evict(key)

        def trans_func(p):
            bool(self.__r.sismember(self.__set_key, key))
        
        is_member = self.__r.transaction(trans_func, *[self.__set_key])

        if not is_member:
            self.__add(key)

    @property
    def maxsize(self):
        """The maximum size of the cache."""
        return self.__maxsize

    @property
    def currsize(self):
        """The current size of the cache."""

        # TODO: Do I need Transaction here? I am not sure yet.
        return self.__r.llen(self.__set_evict_list_key)

    @staticmethod
    def getsizeof(value):
        """Return the size of a cache element's value."""
        return 1

    @property
    def hits(self):
        """Return the # of hits."""
        return int(self.__r.get(self.__hits_key))

    @property
    def misses(self):
        """Return the # of misses"""
        return int(self.__r.get(self.__misses_key)) or 1

    @property
    def cache_info(self):
        return f"{self.__class__.__name__} Cache Info : [" \
               f"Hit Rate %: {100 * self.hits / self.misses}, Hits: {self.hits}," \
               f"Misses: {self.misses}, Size: {self.currsize}]\n"\
               f"[Local cache info: {self.__contains__.cache_info()}]"

    def cache_clear(self):
        """
            Clear FIFOSet data
        """
        self.clean_redis_structures()
        self.__contains__.cache_clear()
        logging.info("[{} cache cleared]".format(self.__class__.__name__))
