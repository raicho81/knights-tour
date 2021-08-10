import logging

import redis
from pottery import RedisDeque, RedisDict, synchronize

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RedisPathsPmsStruct:
    """
        A deque with the paths and there possible moves, which is used as a new task queue.
    """

    def __init__(self,
                 redis_path_pms_list_key,
                 redis_path_pms_hset_key,
                 redis_pool_obj=redis.Redis()):

        self.redis_path_pms_hset_key = redis_path_pms_hset_key
        self.redis_path_pms__list_key = redis_path_pms_list_key
        self.__r = redis_pool_obj
        self.redis_path_pms_hset = RedisDict(redis=self.__r, key=self.redis_path_pms_hset_key)
        self.redis_path_pms_deque = RedisDeque(redis=self.__r, key=self.redis_path_pms__list_key)
        # Use Redis Redlock algo for synchronization between pop and add.
        self.add_path_pms = synchronize(key="knights_tour_modify_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.add_path_pms)
        self.pop_path_pms_from_deque = synchronize(key="knights_tour_modify_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.pop_path_pms_from_deque)
        self.remove_path_from_dict = synchronize(key="knights_tour_modify_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.remove_path_from_dict)        

self.remove_path_from_dict = synchronize(key="knights_tour_modify_pms_celery_task_synchronized", masters={self.__r}, blocking=True, timeout=1)(self.remove_path_from_dict)

    def clean_redis_structures(self):
        self.redis_path_pms_deque.clear()
        self.redis_path_pms_hset.clear()

    # def __repr__(self):
    #     return "{} ({}, currsize={})".format(
    #             self.__class__.__name__,
    #             ["{}: {}".format(k, v) for k, v in self.redis_path_pms_hset.items()],
    #             self.currsize)

    # def __contains__(self, path):
    #     ret = str(path) in self.redis_path_pms_hset
    #     return ret

    def __len__(self):
        return len(self.redis_path_pms_deque)

    def __iter__(self):
        return iter(self.redis_path_pms_hset)

    # def __getitem__(self, path):
    #     return self.redis_path_pms_hset[path]

    def add_path_pms(self, path, pms):
        self.redis_path_pms_list.append(str((path, pms)))
    
    def pop_path_pms_from_deque(self, eval_res_to_python_obj=True):
        try:
            path_pms = self.redis_path_pms_list.pop()
        except IndexError as e:
            print(e)
        if eval_res_to_python_obj:
            path_pms = eval(path_pms)
        self.redis_path_pms_hset[str(path_pms[0])] = str(path_pms[1])
        return path_pms
    
    def remove_path_from_dict(self, path):
        self.redis_path_pms_hset.pop(path)