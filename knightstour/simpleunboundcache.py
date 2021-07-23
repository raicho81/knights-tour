import functools
import math


def very_simple_unbound_board_nodes_cache(f):

    def key(node):
        return (node[1] * wrapper.board_size + node[0]) % wrapper.board_size ** 2

    @functools.wraps(f)
    def wrapper(self, node):
        if not wrapper.cache:
            wrapper.board_size = self.board_size
            wrapper.cache = [None] * wrapper.board_size ** 2

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
        wrapper.hits = 0
        wrapper.misses = 0
        wrapper.board_size = 0
        wrapper.cache = [None] * wrapper.board_size ** 2

    # Init the wrapper internal data
    init_wrapper_data()

    # Add instrumentation to the wrapped function
    wrapper.cache_clear = cache_clear
    wrapper.cache_info = lambda: f"simple_unbound_cache Info : [Cached function: {wrapper.__name__}, " \
                                 f"Hits: {wrapper.hits}, Misses: {wrapper.misses}, Size: {len(wrapper.cache)}"
    return wrapper
