from .celery import app

def set_bits(value, bits):
    for bit in bits:
        value |= (1 << bit)
    return value

@app.task
def make_node_mtx_ctx(path, board_size):
    """
        Compute path's "matrix context pattern" - path nodes are encoded as single bits in a integer.
        The position of the bits set to "1" is relative to the path nodes coordinates.
        This represents the pattern of the given path ignoring the order of the nodes in it
        meaning that the reversed path will have the same matrix pattern and so on.
        This enables fast searches of the paths already known to be with a dead end with minimum required space.
        Keep in mind we just store some integers in a set().
    """
    mtx_ctx = 0
    b = [(path_node[1] * board_size + path_node[0]) for path_node in path]
    mtx_ctx = set_bits(mtx_ctx, b)
    return mtx_ctx


@app.task
def run_any_function_task(f, *args, **kwargs):
    res = f(*args, **kwargs)
    return res
