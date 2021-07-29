from knightstour import FIFOSet
from knightstour import RedisFIFOSet
import redis
import random

s = FIFOSet(maxsize=50, evict_count=10)

r = redis.Redis(host='192.168.1.3', port=6379, password='secret', decode_responses=True)
redis_fifo_set = RedisFIFOSet(maxsize=50,
                              evict_count=10,
                              redis_pool_obj=r,
                              set_key="test_set_1",
                              ev_list_key="test_set_1_ev_list")
for _ in redis_fifo_set:
    print(_)

for _ in range(100):
    rn = random.randint(0, 1000)
    s.add(_)
    redis_fifo_set.add(_)

for x in s:
    if x not in redis_fifo_set:
        raise RuntimeError("Fuck!")

for xx in redis_fifo_set:
    if int(xx) not in s:
        print(xx)
        print(s)
        raise RuntimeError("Fuck!X2")

print(s)
print(redis_fifo_set)
