import functools
import math


def very_simple_unbound_int_cache(_board_size):

    def key(node):
        return (node[1] * _board_size + node[0]) % _board_size ** 2

    def inner(f):

        @functools.wraps(f)
        def wrapper(self, node):
            _key = key(node)
            cached = wrapper.cache[_key]

            if cached:
                wrapper.hits += 1
                return cached
            else:
                wrapper.misses += 1
                res = f(self, node)
                wrapper.cache[_key] = res
                return res

        def cache_clear():
            init_wrapper_data()

        def init_wrapper_data():
            wrapper.cache = [None] * _board_size ** 2
            wrapper.hits = 0
            wrapper.misses = 0

        init_wrapper_data()

        wrapper.cache_clear = cache_clear
        wrapper.cache_info = lambda: f"simple_unbound_cache Info : [Cached function: {wrapper.__name__}, " \
                                     f"Hits: {wrapper.hits}, Misses: {wrapper.misses}, Size: {len(wrapper.cache)}"
        return wrapper

    return inner