import math
from dynaconf import settings
import imp

from celery import Celery
import redis

if (settings.RUNNING_IN_CELERY):
    m = imp.find_module("redisfifoset", ["knightstour"])
    redisfifoset = imp.load_module("redisfifoset", *m)


app = Celery('knights_tour_tasks',
             broker=settings.CELERY_BROKER,
             backend=settings.CELERY_BACKEND,
             include=['celery_tasks.tasks'],
             import_from_cwd=True,
             accept_content=['msgpack', "json"])

app.conf.task_serializer = 'msgpack'
app.conf.result_serializer = 'msgpack'

if (settings.RUNNING_IN_CELERY):
    redis_pool = redis.Redis(host=settings.REDIS_HOST,
                            port=settings.REDIS_PORT,
                            password=settings.REDIS_PASSWORD,
                            decode_responses=True)

    negative_outcome_nodes_cache = redisfifoset.RedisFIFOSet(
                maxsize=settings.NEG_OUTCOMES_CACHE_SIZE,
                evict_count=math.ceil(settings.NEG_OUTCOMES_CACHE_SIZE * settings.PERCENT_TO_EVICT_FROM_NEG_NODES_CACHE / 100.0),
                redis_pool_obj=redis_pool,
                redis_set_key=settings.REDIS_SET_KEY,
                redis_ev_list_key=settings.REDIS_EV_LIST_KEY,
                redis_hits_key=settings.REDIS_HITS_KEY,
                redis_misses_key=settings.REDIS_MISSES_KEY)

if __name__ == '__main__':
    app.start()