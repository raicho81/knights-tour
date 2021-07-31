from knightstour import FIFOSet
from knightstour import RedisFIFOSet
import redis
import random


def run_test(r):
    fifo_set = FIFOSet(maxsize=50, evict_count=10)
    redis_fifo_set = RedisFIFOSet(maxsize=50,
                                  evict_count=10,
                                  redis_pool_obj=r,
                                  redis_set_key="test_set_1",
                                  redis_ev_list_key="test_set_1_ev_list",
                                  redis_hits_key="test_hits_key",
                                  redis_misses_key="test_misses_key")

    import time

    start = time.time()
    to_add = []
    for _ in range(2000):
        to_add.append(_)

    for x in to_add:
        fifo_set.add(x)

    for x in to_add:
        redis_fifo_set.add(x)

    for x in fifo_set:
        if str(x) not in redis_fifo_set:
            raise RuntimeError("{} not in redis_fifo_set!".format(x))

    for xx in redis_fifo_set:
        if int(xx) not in fifo_set:
            raise RuntimeError("{} not in fifo_set".format(xx))

    tt = time.time() - start
    print("time: {} s".format(tt))

    return tt


def main():
    r = redis.Redis(host='192.168.1.3', port=6379, password='secret', decode_responses=True)
    # print(r.execute_command("CLIENT TRACKING ON"))
    runtimes = []

    for tr in range(10):
        runtimes.append(run_test(r))

    import statistics
    print("Avg. runtime: {}".format(statistics.mean(runtimes)))


if __name__ == "__main__":
    main()

