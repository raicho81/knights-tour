from .celery import app, negative_outcome_nodes_cache, redis_paths_pms_hset_deque
from functools import lru_cache

# import redis
from dynaconf import settings


@lru_cache(maxsize=None)
def find_possible_moves_cached(node):
    return find_possible_moves_non_cached(node)


def find_possible_moves_non_cached(node, board_size=settings.BOARD_SIZE):
    possible_moves = []
    # Find all new possible moves by the rules for moving a Knight figure on the Chess desk from a given square.
    # There are at most 8 possible moves from the current square. Of course we must check if a possible move is not
    # out of the desk and is not already in the current path in consideration,
    # for which I am using a set for faster search in the current path.

    new_node = (node[0] + 1, node[1] + 2)
    if new_node[0] < board_size and new_node[1] < board_size:
        possible_moves.append(new_node)
    new_node = (node[0] + 1, node[1] - 2)
    if new_node[0] < board_size and new_node[1] >= 0:
        possible_moves.append(new_node)
    new_node = (node[0] + 2, node[1] + 1)
    if new_node[0] < board_size and new_node[1] < board_size:
        possible_moves.append(new_node)
    new_node = (node[0] + 2, node[1] - 1)
    if new_node[0] < board_size and new_node[1] >= 0:
        possible_moves.append(new_node)
    new_node = (node[0] - 2, node[1] + 1)
    if new_node[0] >= 0 and new_node[1] < board_size:
        possible_moves.append(new_node)
    new_node = (node[0] - 2, node[1] - 1)
    if new_node[0] >= 0 and new_node[1] >= 0:
        possible_moves.append(new_node)
    new_node = (node[0] - 1, node[1] + 2)
    if new_node[0] >= 0 and new_node[1] < board_size:
        possible_moves.append(new_node)
    new_node = (node[0] - 1, node[1] - 2)
    if new_node[0] >= 0 and new_node[1] >= 0:
        possible_moves.append(new_node)
    return possible_moves


def drop_out_moves_in_path(moves, path):
    return [x for x in moves if x not in path]


def find_possible_moves(path, enable_cache=True):
    if enable_cache:
        return drop_out_moves_in_path(find_possible_moves_cached(path[-1]), path)
    else:
        return drop_out_moves_in_path(find_possible_moves_non_cached(path[-1]), path)


def set_bits(value, bits):
    for bit in bits:
        value |= (1 << bit)
    return value



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


def compute_mtx_ctx(path):
    mtx_ctx = make_node_mtx_ctx(path, settings.BOARD_SIZE)
    return mtx_ctx


def check_negative_node_previous_node_pms_and_cache(self, path):
    while len(path) >= settings.MIN_NEG_PATH_LENGTH:
        node = path[-1]
        path = path[:-1]
        pms = find_possible_moves(path)
        if not pms:
            path.append(node)
            raise RuntimeError("Previous node in the path doesn't have possible moves! path: {}".format(path))
        if len(pms) > 1:
            break
        self.add_to_negative_outcome_nodes_cache(path)
    self.add_to_negative_outcome_nodes_cache(path)


def add_to_negative_outcome_nodes_cache(dead_end_path):
    mtx_ctx = compute_mtx_ctx(dead_end_path)
    negative_outcome_nodes_cache.add(mtx_ctx)


def check_if_path_found(self, new_path):
    if len(new_path) == self.board_size * self.board_size:
        if self.run_time_checks:
            try:
                self.check_path(new_path)
                self.found_walks_count += 1
            except RuntimeError as rte:
                print(rte)
        else:
            self.found_walks_count += 1
        print("Path#{}:{}".format(self.found_walks_count, self.make_walk_path_string(new_path)))
        return True
    return False


def find_new_pms_and_dead_ends(new_path, current_new_paths_pms):
    new_pms = find_possible_moves(new_path)
    if settings.ENABLE_CACHE and new_pms and len(new_path) >= settings.MIN_NEG_PATH_LENGTH:
        for new_pm_node in new_pms:
            new_path.append(new_pm_node)
            mtx_ctx = compute_mtx_ctx(new_path)
            if mtx_ctx in negative_outcome_nodes_cache:     # This goes to Redis too - it will be shared by all the workers.
                new_pms.remove(new_pm_node)                 # It really beats me why this is working correctly. Try changing it to something else ... Like saving the values you have to
                                                            # remove and remove them later and observe the results.
            new_path.pop()
    if new_pms:
        current_new_paths_pms.append((new_path[-1], new_pms)) # I guess I have to return current_new_paths_pms as a result of the function
    else:
        settings.ENABLE_CACHE and check_negative_node_previous_node_pms_and_cache(new_path)

def find_walks_celery_task():
    found_paths = []
    while True:
        current_path = redis_paths_pms_hset_deque.pop_path_from_deque()
        possible_moves = redis_paths_pms_hset_deque[str(current_path)] if current_path else []
        for possible_move in possible_moves:
            current_path.append(possible_move)
            if check_if_path_found(current_path):
                found_paths.append(current_path)
                current_path.pop()
                continue
            find_new_pms_and_dead_ends(current_path, current_new_paths_pms)
            current_path.pop()
        if not current_new_paths_pms:
            redis_paths_pms_hset_deque.remove_path_from_dict(str(current_path))
            return
        if not settings.BRUTE_FORCE:
            # Skip nodes with more possible outcomes than the first node in the sorted list if brute_forse is OFF
            # Warnsdorff's rule https://en.wikipedia.org/wiki/Knight%27s_tour#Warnsdorff's_rule
            current_new_paths_pms = sorted(current_new_paths_pms, key=lambda x: len(x[1]))
            if current_new_paths_pms:
                cur_min_pms_len = len(current_new_paths_pms[0][1])
        for new_path_possible_moves in current_new_paths_pms:
            if not settings.BRUTE_FORCE and len(new_path_possible_moves[1]) > cur_min_pms_len:
                break
            current_path.append(new_path_possible_moves[0])
            redis_paths_pms_hset_deque.add_path_pms(current_path, new_path_possible_moves[1])
            current_path.pop()
        if found_paths:
            return found_paths


@app.task
def run_any_function_task(f, *args, **kwargs):
    res = f(*args, **kwargs)
    return res
