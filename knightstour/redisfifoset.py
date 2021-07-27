import redis
import logging

KTNOS = "knights_tour_negative_outcomes_set_cache"
KTNOEL = "knights_tour_negative_outcomes_evict_list"


class RedisFIFOSet:
    """
        Class representing a set, which can be bound in size. When the maximum size is reached the first
        elements added equal to evict_count are removed to make place for new ones or if the elements are less than
        evict_count then only the available are evicted resulting in an empty set.
        If maxsize is None the set behaves like an ordinary set(). Stored in Redis.
    """

    def __init__(self, maxsize=None, evict_count=1000, redis_obj=redis.Redis()):
        self.__maxsize = maxsize
        self.__currsize = 0
        self.__evict_count = evict_count
        self.__hits = 0
        self.__misses = 0
        self.__set = KTNOS
        self.__set_evict_list = KTNOEL
        self.r = redis_obj
        if not self.r:
            raise ValueError("Invalid Redis connection!")
        self.delete_redis_structures()

    def delete_redis_structures(self):
        logging.debug(self.r.delete(self.__set, self.__set_evict_list))

    def __contains__(self, key):
        ret = self.r.sismember(self.__set, key)
        # logging.debug("__contains__, key: {} = {}".format(key, ret))
        if ret:
            self.__hits += 1
        else:
            self.__misses += 1
        return ret

    def __iter__(self):
        return iter(self.r.sscan(self.__set, 0))

    def __len__(self):
        return self.r.scard(self.__set)

    def add(self, key):
        if self.__maxsize and (self.__currsize + self.getsizeof(key)) > self.__maxsize:

            how_much_to_evict = min(self.__evict_count, self.__currsize)
            to_evict = []

            with self.r.pipeline(transaction=True) as p:
                for _ in range(how_much_to_evict):
                    to_evict.append(self.r.rpop(self.__set_evict_list))

                for elm_to_evict in to_evict:
                    self.r.srem(self.__set, elm_to_evict)
                try:
                    p.execute()
                    self.__currsize -= len(to_evict)
                except Exception as e:
                    logging.error(e)

        if not self.r.sismember(self.__set, key):
            with self.r.pipeline(transaction=True) as p:
                p.lpush(self.__set_evict_list, key)
                p.sadd(self.__set, key)
                try:
                    p.execute()
                    self.__currsize += self.getsizeof(key)
                except Exception as e:
                    logging.error(e)

    @property
    def maxsize(self):
        """The maximum size of the cache."""
        return self.__maxsize

    @property
    def currsize(self):
        """The current size of the cache."""
        return self.__currsize

    @staticmethod
    def getsizeof(value):
        """Return the size of a cache element's value."""
        return 1

    @property
    def hits(self):
        """Return the # of hits."""
        return self.__hits

    @property
    def misses(self):
        """Return the # of misses"""
        return self.__misses

    @property
    def cache_info(self):
        return f"RedisFIFOSet Cache Info : [Hit Rate %: {100 * self.__hits / (self.__misses or 1)}, Hits: {self.__hits}," \
               f"Misses: {self.__misses}, Size: {self.__currsize}]"

    def cache_clear(self):
        """
            Clear FIFOSet data
        """
        self.delete_redis_structures()
        self.__currsize = 0
        self.__hits = 0
        self.__misses = 0
        logging.info("[Redis FIFOSet cache cleared]")
