import logging
from typing import Union, Any

import crc16
import redis
import itertools


class RedisPathsPmsHSet:
    """
        Class representing a hash set with paths and their respective possible moves.
        There is also a list with the HSet slots and keys, which is used as a new task queue
    """

    def __init__(self, redis_path_pms_hset_key,
                 redis_path_pms__list_key,
                 redis_pool_obj=redis.Redis(),
                 redis_cache_slots_count=8):
        self.r = redis_pool_obj
        self.redis_path_pms__list_key = redis_path_pms__list_key
        self.redis_path_pms_hset_key = redis_path_pms_hset_key
        self.cache_slots_count = redis_cache_slots_count
        self.current_cache_slot_n = -1
        self.current_slot_local_cpy = {}
        if not self.r:
            raise ValueError("Redis connection pool object is None!")
        self.clean_redis_structures()

    def clean_redis_structures(self):
        p = self.r.pipeline(transaction=True)
        p.delete(self.redis_path_pms__list_key)
        for sn in range(0, self.cache_slots_count):
            slot = "{}_slot_{}".format(self.redis_path_pms__list_key, sn)
            p.delete(slot)
        p.execute()

    def __repr__(self):
        return "{} ({}, currsize={})".format(
            self.__class__.__name__,
            ["{}: {}".format(k, v) for k, v in itertools.chain(
                *[self.r.sscan_iter("{}_slot_{}".format(self.redis_path_pms__list_key, sn)).items() for sn in range(0, self.cache_slots_count)])],
            self.currsize
            )

    def __contains__(self, key):
        slot_n: Union[int, Any] = self.slot_n(bytes(key)) % self.cache_slots_count

        if slot_n != self.current_cache_slot_n:
            self.current_cache_slot_n = slot_n
            slot = "{}_slot_{}".format(self.redis_path_pms_hset_key, self.current_cache_slot_n)
            slot_content_iter = self.r.hscan_iter(slot)
            self.current_slot_local_cpy = dict(slot_content_iter)
            logging.info("Loaded cache slot: {} into self.current_slot_local_cpy".format(slot))

        ret = str(key) in self.current_slot_local_cpy
        return ret

    def __iter__(self):
        return itertools.chain(*[self.r.sscan_iter("{}_slot_{}".format(self.redis_path_pms__list_key, sn)).items()
                                 for sn in range(0, self.cache_slots_count > 0 or 1)])

    def __len__(self):
        return self.r.llen(self.redis_path_pms__list_key)

    def slot_n(self, key):
        crc_16 = crc16.crc16xmodem(bytes(key), 0xFFFF)
        sn = crc_16 % self.cache_slots_count
        return sn

    def add(self, key):
        is_key_present = key in self
        if not is_key_present:
            with self.r.pipeline(transaction=True) as p:
                slot_n = self.slot_n(bytes(key)) % self.cache_slots_count
                slot = "{}_slot_{}".format(self.redis_path_pms_hset_key, slot_n)
                p.sadd(slot, key)
                p.lpush(self.redis_path_pms__list_key, key)
                try:
                    p.execute()
                    if slot_n == self.current_cache_slot_n:
                        self.current_slot_local_cpy.add(str(key))
                except BrokenPipeError as e:
                    logging.error(e)

    @property
    def currsize(self):
        """The current size of the cache."""
        return len(self)

    @staticmethod
    def getsizeof(value):
        """Return the size of a cache element's value."""
        return 1

    def cache_clear(self):
        """
            Clear data
        """
        self.clean_redis_structures()
        logging.info("[Redis {} cache cleared]".format(self.__class__.__name__))
