import functools
import logging


def simple_unbound_cache(f):
    """
        Very simple (and hopefully very fast) unbound cache of the nodes possible moves.
        Realised as a simple list with a very simple hash function to map the input (node) to the output list (moves)  with a <=> (equivalence) relation.
        No need to pre-build LUT tables as this can be computed in constant time (board_size_x * board_size_y, provided that board_size is a small constant)
    """
    def key(node):
        return (node[1] * wrapper.board_size_x + node[0])

    @functools.wraps(f)
    def wrapper(self, node):
        if not wrapper.cache:
            wrapper.board_size_x = self.board_size_x
            wrapper.board_size_y = self.board_size_y
            wrapper.cache = [None] * (wrapper.board_size_x * wrapper.board_size_y)
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
        logging.info("[very_simple_unbound_board_nodes_lut_cache cleared]")

    def init_wrapper_data():
        wrapper.hits = 0
        wrapper.misses = 0
        wrapper.board_size_x = 0
        wrapper.board_size_y = 0
        wrapper.cache = [None] * (wrapper.board_size_x * wrapper.board_size_y)

    # Init the wrapper internal data
    init_wrapper_data()
    # Add instrumentation to the wrapped function
    wrapper.cache_clear = cache_clear
    wrapper.cache_info = lambda: f"Very, Very Simple Unbound Node Possible Moves Cache Info: [Cached function: {wrapper.__name__}, " \
                                 f"Hits %: {wrapper.hits * 100 / (wrapper.hits + wrapper.misses)}, Hits: {wrapper.hits}, Misses: {wrapper.misses}, Size: {len(wrapper.cache)}"
    return wrapper
