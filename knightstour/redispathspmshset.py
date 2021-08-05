import logging

import redis
from pottery import RedisDeque, RedisDict, synchronize

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RedisPathsPmsHSet:
    """
        Class representing a hash set with paths and their respective possible moves.
        There is also a deque with the HSet keys, which is used as a new task queue.
    """

    def __init__(self, redis_path_pms_hset_key,
                 redis_path_pms_list_key,
                 redis_pool_obj=redis.Redis()):
        self.redis_path_pms__list_key = redis_path_pms_list_key
        self.redis_path_pms_hset_key = redis_path_pms_hset_key
        self.__r = redis_pool_obj
        self.redis_path_pms_list = RedisDeque(redis=self.__r, key=self.redis_path_pms__list_key)
        self.redis_path_pms_hset = RedisDict(redis=self.__r, key=self.redis_path_pms_hset_key)

        # Use Redis Redlock algo for synchronization between threads and different machines etc.
        self.add_path_pms = synchronize(key="knights_tour_add_path_pms_path_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.add_path_pms)
        self.__iter__ = synchronize(key="knights_tour___iter___path_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.__iter__)
        self.__len__ = synchronize(key="knights_tour___len___path_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.__len__)
        self.__contains__ = synchronize(key="knights_tour___contains___path_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.__contains__)
        self.clean_redis_structures = synchronize(key="knights_tour_clean_redis_structures_path_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.clean_redis_structures)
        self.__getitem__ = synchronize(key="knights_tour___getitem___path_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.__getitem__)

    def clean_redis_structures(self):
        self.redis_path_pms_hset.clear()
        self.redis_path_pms_list.clear()

    def __repr__(self):
        return "{} ({}, currsize={})".format(
                self.__class__.__name__,
                ["{}: {}".format(k, v) for k, v in self.redis_path_pms_hset.items()],
                self.currsize)

    def __contains__(self, path):
        ret = str(path) in self.redis_path_pms_hset
        return ret

    def __len__(self):
        return len(self.redis_path_pms_hset)

    def __iter__(self):
        return iter(self.redis_path_pms_hset)

    def __getitem__(self, path):
        return self.redis_path_pms_hset[path]

    def add_path_pms(self, path, pms):
        if not isinstance(path, str) or not isinstance(pms, str):
            raise ValueError("Invalid key and value - must be strings")
        is_key_present = path in self
        if not is_key_present:
            self.redis_path_pms_hset[path] = pms
            self.redis_path_pms_list.append(path)
    
    def pop_path_from_deque(self, eval_res_to_python_obj=True):
        path = "[]"
        try:
            path = self.redis_path_pms_list.pop()
        except IndexError as e:
            print(e)
        if eval_res_to_python_obj:
            path = eval(path)
        return path

    def remove_path_from_dict(self, path, eval_res_to_python_obj=True):
        pms = self.redis_path_pms_hset.pop(path)
        if eval_res_to_python_obj:
            pms = eval(pms)        
        return pms

    @property
    def currsize(self):
        """The current size of the Hash Queue."""
        return len(self)
