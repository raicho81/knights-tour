import math
from string import ascii_lowercase as ascii_lc
import time
import logging

import functools

from .simpleunboundcache import simple_unbound_cache
from knightstour import FIFOSet


class KnightsTourAlgo:
    """
        Algo for finding all knights tours by brute force with some caching implemented for speeding up the
        solutions. Also there is a non brute force version implemented, which is using Warnsdorff's rule
        https://en.wikipedia.org/wiki/Knight%27s_tour#Warnsdorff's_rule. Switching between them is done via the [[brute_force]] parameter.
    """
    def __init__(self, board_size_x, board_size_y, brute_force=False, run_time_checks=True, min_negative_path_len=2,
                 negative_outcome_nodes_max_cache_size=10 * 1000 * 1000, percent_to_evict=3):
        self.board_size_x = board_size_x
        self.board_size_y = board_size_y
        self.found_walks_count = 0
        self.brute_force = brute_force
        self.algo_start_time = time.time()
        self.negative_outcome_nodes_max_cache_size = negative_outcome_nodes_max_cache_size
        self.negative_outcome_nodes_cache = FIFOSet(maxsize=self.negative_outcome_nodes_max_cache_size,
                                                    evict_count=math.ceil(negative_outcome_nodes_max_cache_size * percent_to_evict / 100.0))
        self.generated_paths_set = set()
        self.run_time_checks = run_time_checks
        self.min_negative_path_len = min_negative_path_len

    def init_internal_data(self):
        self.algo_start_time = time.time()
        self.generated_paths_set = set()
        self.found_walks_count = 0
        self.negative_outcome_nodes_cache.clear()
        self.find_possible_moves_helper.cache_clear()

    def negative_outcomes_cache_size(self):
        return self.negative_outcome_nodes_cache.currsize

    def seconds_to_str(self, t):
        return "%02d:%02d:%02d.%03d" % \
               functools.reduce(lambda ll, b: divmod(ll[0], b) + ll[1:],
                                [(round(t * 1000),), 1000, 60, 60])

    def drop_out_moves_in_path(self, moves, path):
        return [x for x in moves if x not in path]

    @simple_unbound_cache
    def find_possible_moves_helper(self, node):
        possible_moves = []
        # Find all new possible moves by the rules for moving a Knight figure on the Chess desk from a given square.
        # There are at most 8 possible moves from the current square. Of course we must check if a possible move is not
        # out of the desk and is not already in the current path in consideration,
        # for which I am using a set for faster search in the current path.
        new_node = (node[0] + 1, node[1] + 2)
        if new_node[0] < self.board_size_x and new_node[1] < self.board_size_y:
            possible_moves.append(new_node)
        new_node = (node[0] + 1, node[1] - 2)
        if new_node[0] < self.board_size_x and new_node[1] >= 0:
            possible_moves.append(new_node)
        new_node = (node[0] + 2, node[1] + 1)
        if new_node[0] < self.board_size_x and new_node[1] < self.board_size_y:
            possible_moves.append(new_node)
        new_node = (node[0] + 2, node[1] - 1)
        if new_node[0] < self.board_size_x and new_node[1] >= 0:
            possible_moves.append(new_node)
        new_node = (node[0] - 2, node[1] + 1)
        if new_node[0] >= 0 and new_node[1] < self.board_size_y:
            possible_moves.append(new_node)
        new_node = (node[0] - 2, node[1] - 1)
        if new_node[0] >= 0 and new_node[1] >= 0:
            possible_moves.append(new_node)
        new_node = (node[0] - 1, node[1] + 2)
        if new_node[0] >= 0 and new_node[1] < self.board_size_y:
            possible_moves.append(new_node)
        new_node = (node[0] - 1, node[1] - 2)
        if new_node[0] >= 0 and new_node[1] >= 0:
            possible_moves.append(new_node)
        return possible_moves

    def find_possible_moves(self, node, path):
        return self.drop_out_moves_in_path(self.find_possible_moves_helper(node), path)

    @staticmethod
    def make_walk_path_string(walk):
        node = walk[0]
        walk_string = "{}{}".format(ascii_lc[node[0]], node[1] + 1)
        for node in walk[1:]:
            walk_string = "{}{}{}".format(walk_string, ascii_lc[node[0]], node[1] + 1)
        return walk_string

    def clear_bit(self, value, bit):
        return value & ~(1 << bit)

    def set_bits(self, bits):
        value = 0
        for bit in bits:
            value |= bit
        return value

    def make_node_mtx_ctx(self, path):
        """
            Compute path's "matrix context pattern" - path nodes are encoded as single bits in a integer.
            The position of the bits set to "1" is relative to the path nodes coordinates.
            This represents the pattern of the given path ignoring the order of the nodes in it meaning that the reversed path will have the same
            matrix pattern and so on. This enables fast searches of the paths already known to be with a dead end with minimum required space.
            Keep in mind we just store some integers in a set().
        """
        mtx_ctx = 0
        for path_node in path:
            mtx_ctx |= (1 << (path_node[1] * self.board_size_x + path_node[0]))
        return mtx_ctx

    def check_negative_node_previous_node_pms_and_cache(self, path):
        while len(path) >= self.min_negative_path_len:
            node = path[-1]
            path = path[:-1]
            pms = self.find_possible_moves(path[-1], path)
            if not pms:
                path.append(node)
                raise RuntimeError("[Previous node in the path doesn't have possible moves! path: {}]".format(path))
            if len(pms) > 1:
                break
            self.add_to_negative_outcome_nodes_cache(path)
        self.add_to_negative_outcome_nodes_cache(path)

    def add_to_negative_outcome_nodes_cache(self, dead_end_path):
        mtx_ctx = self.compute_mtx_ctx(dead_end_path)
        self.negative_outcome_nodes_cache.add(mtx_ctx)

    def compute_mtx_ctx(self, path):
        mtx_ctx = self.make_node_mtx_ctx(path)
        return mtx_ctx

    def check_path(self, path):
        if len(path) != self.board_size_x * self.board_size_y:
            raise RuntimeError("[Invalid path length: {},  path: {}. Must be: {}]".format(len(path), path, self.board_size_x * self.board_size_y))
        node = path[0]
        pms = self.find_possible_moves(node, path[0:1])
        for next_node_idx in range(1, len(path)):
            next_node = path[next_node_idx]
            pms = self.find_possible_moves(node, path[0:next_node_idx])
            if next_node not in pms:
                raise RuntimeError(
                    "[Invalid path! There are no possible moves from the previous node to the next > path: {} > node: {} > pms: {} > next_node: {}]"
                        .format(path, node, pms, next_node))
            node = next_node
        if tuple(path) in self.generated_paths_set:
            raise RuntimeError("[Path is already generated! path: {}, node: {}, pms: {}, next_node: {}]"
                               .format(path, node, pms, next_node))
        self.generated_paths_set.add(tuple(path))

    def print_info(self, what):
        logging.info("[{} self.negative_outcome_nodes_cache Info: {}]".format(
            what,
            self.negative_outcome_nodes_cache.cache_info))
        logging.info("[{} self.find_possible_moves_helper Info: {}]".format(
            what,
            self.find_possible_moves_helper.cache_info()))

    def check_if_path_found(self, new_path):
        if len(new_path) == self.board_size_x * self.board_size_y:
            if self.run_time_checks:
                try:
                    self.check_path(new_path)
                    self.found_walks_count += 1
                except RuntimeError as rte:
                    logging.exception(rte)
            else:
                self.found_walks_count += 1
            if self.found_walks_count and self.found_walks_count % 100 == 0:
                self.print_info(what="Current")
            logging.info("[Path#{}: {}]".format(self.found_walks_count, self.make_walk_path_string(new_path)))
            return True
        return False

    def find_new_pms_and_dead_ends(self, new_path, current_new_paths_pms):
        new_pms = self.find_possible_moves(new_path[-1], new_path)
        if new_pms and len(new_path) >= self.min_negative_path_len:
            for new_pm_node in new_pms:
                new_path.append(new_pm_node)
                mtx_ctx = self.compute_mtx_ctx(new_path)
                if mtx_ctx in self.negative_outcome_nodes_cache:
                    new_pms.remove(new_pm_node)
                new_path.pop()
        if new_pms:
            current_new_paths_pms.append((new_path[-1], new_pms))
        else:
            self.check_negative_node_previous_node_pms_and_cache(new_path)

    def find_walks(self, current_path, possible_moves):
        current_new_paths_pms = []
        for possible_move in possible_moves:
            current_path.append(possible_move)
            if self.check_if_path_found(current_path):
                current_path.pop()
                continue
            self.find_new_pms_and_dead_ends(current_path, current_new_paths_pms)
            current_path.pop()
        if not current_new_paths_pms:
            return
        if not self.brute_force:
            current_new_paths_pms = sorted(current_new_paths_pms, key=lambda x: len(x[1]))
            if current_new_paths_pms:
                cur_min_pms_len = len(current_new_paths_pms[0][1])
        for new_path_possible_moves in current_new_paths_pms:
            if not self.brute_force and len(new_path_possible_moves[1]) > cur_min_pms_len:
                break
            current_path.append(new_path_possible_moves[0])
            self.find_walks(current_path, new_path_possible_moves[1])
            current_path.pop()

    def print_all_walks_info(self):
        logging.info("[# of possible walks found so far: {}]".format(self.found_walks_count))
        self.print_info(what="Final")

    def bootstrap_search(self):
        logging.info("[Start search]".format(self.found_walks_count))
        possible_moves = []
        for x_coord in range(self.board_size_x):
            for y_coord in range(self.board_size_y):
                start_node = (x_coord, y_coord)
                start_path = [start_node]
                pms = self.find_possible_moves(start_node, start_path)
                possible_moves.append((start_node, pms))
        if not self.brute_force:
            possible_moves = sorted(possible_moves, key=lambda x: len(x[1]))
            if possible_moves:
                possible_move_min_len = len(possible_moves[0][1])
        for pm in possible_moves:
            if not self.brute_force and len(possible_moves[0][1]) > possible_move_min_len:
                break
            self.find_walks([pm[0]], pm[1])

    def run(self):
        logging.info("[*** ALGO PARAMETERS START ***]")
        logging.info("[Board size: {}x{}]".format(self.board_size_x, self.board_size_y))
        logging.info("[Brute force: {}]".format(self.brute_force))
        logging.info("[Run time checks: {}]".format(self.run_time_checks))
        logging.info("[Min negative path len: {}]".format(self.min_negative_path_len))
        logging.info("[Negative outcome nodes max cache size: {}]".format(self.negative_outcome_nodes_max_cache_size))
        logging.info("[*** ALGO PARAMETERS END ***]")
        logging.info("[Clearing caches]")
        self.find_possible_moves_helper.cache_clear()
        self.negative_outcome_nodes_cache.cache_clear()
        logging.info("[Caches cleared]")
        self.bootstrap_search()
        tt = time.time() - self.algo_start_time
        self.print_all_walks_info()
        logging.info("*** ALGO TOTAL TIME: {}s ***".format(self.seconds_to_str(tt)))
        logging.info("*** ALGO END ***".format())
        return tt, tt / (self.found_walks_count or 1)
