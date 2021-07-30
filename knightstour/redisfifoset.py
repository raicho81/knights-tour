import logging


class RedisFIFOSetIterator:

    def __init__(self, fifoset_ev_list, r):
        self.pos = 0
        self.evict_list_key = fifoset_ev_list
        self.r = r
        self.len = self.r.llen(self.evict_list_key)

    def __next__(self):
        while self.pos < self.len:
            ret = self.r.lindex(self.evict_list_key, self.pos)
            self.pos += 1
            return ret

        raise StopIteration

    def __iter__(self):
        return RedisFIFOSetIterator(self.evict_list_key, self.r)


class RedisFIFOSet:
    """
        Class representing a set, which can be bound in size. When the maximum size is reached the first
        elements added equal to evict_count are removed to make place for new ones or if the elements are less than
        evict_count then only the available are evicted resulting in an empty set.
        If maxsize is None the set behaves like an ordinary set() with the extra load of
        a FIFO list for the SET keys Stored in Redis.
    """

    def __init__(self, maxsize=None, evict_count=1000, redis_pool_obj=None, redis_set_key=None, redis_ev_list_key=None,
                 redis_hits_key=None, redis_misses_key=None, cache_slots_count=4):
        self.__hits_key = redis_hits_key
        self.__misses_key = redis_misses_key
        self.__maxsize = maxsize
        self.__evict_count = evict_count
        self.__set_key = redis_set_key
        self.__set_evict_list_key = redis_ev_list_key
        self.r = redis_pool_obj
        self.cache_slots_count = cache_slots_count
        self.current_cache_slot_n = -1
        self.current_slot_local_cpy = {}
        if not self.r:
            raise ValueError("Redis connection pool object is None!")
        self.clean_redis_structures()

    def clean_redis_structures(self):
        p = self.r.pipeline(transaction=True)
        p.delete(self.__set_evict_list_key, self.__hits_key, self.__misses_key)
        for sn in range(0, self.cache_slots_count):
            slot = "{}_slot_{}".format(self.__set_key, sn)
            p.delete(slot)
        p.set(self.__hits_key, 0)
        p.set(self.__misses_key, 0)
        p.execute()

    def __repr__(self):
        return "{} ({}, maxsize={}, currsize={}, hits={}, misses={}, evict_count={})".format(
            self.__class__.__name__,
            [key for key in self],
            self.__maxsize,
            self.currsize,
            self.hits,
            self.misses,
            self.__evict_count
        )

    def __contains__(self, key):
        slot_n = key % self.cache_slots_count

        if slot_n != self.current_cache_slot_n:
            self.current_cache_slot_n = slot_n
            slot = "{}_slot_{}".format(self.__set_key, self.current_cache_slot_n)
            slot_content_iter = self.r.sscan_iter(slot)
            self.current_slot_local_cpy = set(slot_content_iter)
            logging.info("Loaded cache slot: {} into self.current_slot_local_cpy".format(slot))

        ret = str(key) in self.current_slot_local_cpy

        if ret:
            self.r.incr(self.__hits_key)
        else:
            self.r.incr(self.__misses_key)

        return ret

    # def __iter__(self):
    #     return RedisFIFOSetIterator(self.__set_evict_list_key, self.r)

    def __len__(self):
        return self.r.llen(self.__set_evict_list_key)

    def add(self, key):
        currsize = self.currsize

        if self.__maxsize and (currsize + self.getsizeof(key)) > self.__maxsize:
            how_much_to_evict = min(self.__evict_count, currsize)
            llen = self.r.llen(self.__set_evict_list_key)
            to_evict = self.r.lrange(self.__set_evict_list_key, llen - how_much_to_evict, llen)

            with self.r.pipeline(transaction=True) as p:
                for elm_to_evict in to_evict:
                    slot_n = elm_to_evict % self.cache_slots_count
                    slot = "{}_slot_{}".format(self.__set_key, slot_n)
                    if self.current_cache_slot_n == slot_n:
                        self.current_slot_local_cpy.pop(key)
                    self.r.srem(slot, elm_to_evict)
                    self.r.lpop(self.__set_evict_list_key)
                try:
                    p.execute()
                except Exception as e:
                    logging.error(e)

        is_key_present = key in self
        if not is_key_present:
            with self.r.pipeline(transaction=True) as p:
                slot_n = key % self.cache_slots_count
                slot = "{}_slot_{}".format(self.__set_key, slot_n)
                p.sadd(slot, key)
                p.lpush(self.__set_evict_list_key, key)
                try:
                    p.execute()
                    if slot_n == self.current_cache_slot_n:
                        self.current_slot_local_cpy.add(str(key))
                except Exception as e:
                    logging.error(e)

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
        return int(self.r.get(self.__hits_key))

    @property
    def misses(self):
        """Return the # of misses"""
        return int(self.r.get(self.__misses_key)) or 1

    @property
    def cache_info(self):
        return f"RedisFIFOSet Cache Info : [" \
               f"Hit Rate %: {100 * self.hits / self.misses}, Hits: {self.hits}," \
               f"Misses: {self.misses}, Size: {self.currsize}]"

    def cache_clear(self):
        """
            Clear FIFOSet data
        """
        self.clean_redis_structures()
        logging.info("[Redis FIFOSet cache cleared]")