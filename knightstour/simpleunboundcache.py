import functools


def very_simple_unbound_int_cache(_board_size=5):
    """
    # Quite simple unbound cache for function with ONLY positional arguments.
    """

    def key(node):
        return node[1] * _board_size + node[0]

    def inner(f):
        @functools.wraps(f)
        def wrapper(self, *args):
            _key = wrapper.key(*args)
            cached = wrapper.cache[_key]
            if cached:
                wrapper.hits += 1
                return cached
            else:
                wrapper.misses += 1
                res = f(self, *args)
                wrapper.cache[_key] = res
                return res

        def cache_clear():
            wrapper.cache.clear()
            wrapper.hits = 0
            wrapper.misses = 0

        wrapper.key = key
        wrapper.hits = 0
        wrapper.misses = 0
        wrapper.cache = [None] * _board_size ** 2

        wrapper.clear_cache = cache_clear

        wrapper.cache_info = lambda: f"simple_unbound_cache Info : [Cached function: {wrapper.__name__}, " \
                                     f"Hits: {wrapper.hits}, Misses: {wrapper.misses}, Size: {len(wrapper.cache)}"
        return wrapper

    return inner