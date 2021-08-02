import cachetools.func
import logging

import redis
from pottery import synchronize

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RedisFIFOSet:
    
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
        
        # Sunchronize access to critical functions with Redis Redlock
        self.clean_redis_structures = synchronize(key='knights_tour_synchronized_clean_redis_structures', masters={self.__r}, auto_release_time=1000, blocking=True, timeout=-1)(self.clean_redis_structures)
        self.add = synchronize(key='knights_tour_synchronized_add', masters={self.__r}, blocking=True, timeout=1000)(self.add)

    def clean_redis_structures(self):
            self.__r.delete(*[self.__set_evict_list_key, self.__set_key])
            self.__r.set(self.__hits_key, 0)
            self.__r.set(self.__misses_key, 0)

    def __repr__(self):
        # s = [key for key in self]

        # def trans_func(p):
        #     return p.lrange(self.__set_evict_list_key, 0, -1)

        # l = self.__r.transaction(trans_func, *[self.__set_evict_list_key]) 
        return "{} (maxsize={}, currsize={}, hits={}, misses={}, evict_count={})".format(
            self.__class__.__name__,
            self.__maxsize,
            self.currsize,
            self.hits,
            self.misses,
            self.__evict_count
        )

    @cachetools.func.lru_cache(maxsize=32688)
    def __contains__(self, key):
        ret = self.__r.transaction(lambda p: bool(p.sismember(self.__set_key, key)), *[self.__set_key], value_from_callable=True)

        if ret:
            self.__r.incr(self.__hits_key)
        else:
            self.__r.incr(self.__misses_key)
            self.add(key, is_member=ret)
        
        return ret

    def __iter__(self):
        # TODO: Do I need Transaction/Redlock here? I am not sure yet.
        return  iter(self.__r.sscan_iter(self.__set_key))

    def __len__(self):
        def trans_func_llen(p):
            l = int(p.llen(self.__set_evict_list_key))
            return l
        
        ll = self.__r.transaction(trans_func_llen, *[self.__set_evict_list_key], value_from_callable=True)
    
        return ll
        
    def __evict(self, key):
        cursz = self.currsize
        if self.__maxsize and (cursz + self.getsizeof(key)) <= self.__maxsize:
            return

        how_much_to_evict = min(self.__evict_count, cursz)
        llen = self.__r.llen(self.__set_evict_list_key)
        to_evict = self.__r.lrange(self.__set_evict_list_key, llen - how_much_to_evict, llen)

        for elm_to_evict in to_evict:
            self.__r.rpop(self.__set_evict_list_key)
            self.__r.srem(self.__set_key, elm_to_evict)
            self.__contains__.pop(key)

    def __add(self, key):
        self.__r.sadd(self.__set_key, key)
        self.__r.lpush(self.__set_evict_list_key, key)

    def add(self, key, is_member=None):
        if is_member is None:
            is_member = bool(self.__r.sismember(self.__set_key, key))

        self.__evict(key)
        if not is_member:
            self.__add(key)
    
    @property
    def maxsize(self):
        """The maximum size of the cache."""
        return self.__maxsize

    @property
    def currsize(self):
        """The current size of the cache."""

        return len(self)

    @staticmethod
    def getsizeof(value):
        """Return the size of a cache element's value."""
        
        return 1

    @property
    def hits(self):
        """Return the # of hits."""

        return int(self.__r.transaction(lambda p: p.get(self.__hits_key), *[self.__hits_key], value_from_callable=True))

    @property
    def misses(self):
        """Return the # of misses"""

        return int(self.__r.transaction(lambda p: p.get(self.__misses_key), *[self.__misses_key], value_from_callable=True)) or 1

    @property
    def cache_info(self):
        return f"{self.__class__.__name__} Cache Info : [" \
               f"Hit Rate %: {100 * self.hits / self.misses}, Hits: {self.hits}," \
               f"Misses: {self.misses}, Size: {self.currsize}]\n"\
               f"[Local cache info: {self.__contains__.cache_info()}]"

    def cache_clear(self):
        """
            Clear caches data
        """
        self.clean_redis_structures()
        self.__contains__.cache_clear()
        logger.info("[{} cache cleared]".format(self.__class__.__name__))
