import functools
import sys
import statistics
from string import ascii_lowercase as ascii_lc
import time
import logging
import cachetools
import functools


logging.basicConfig(filename=__file__ + ".log",
                    filemode="a+",
                    # encoding='utf-8',
                    format='[%(asctime)s] [%(levelname)s]: %(message)s',
                    level=logging.DEBUG)


class KnightsTourAlgo:
    """
        Algo for finding all knights tours by brute force with some caching implemented for speeding up the
        solutions. Also there is a non brute force version implemented, which is using Warnsdorff's rule
        https://en.wikipedia.org/wiki/Knight%27s_tour#Warnsdorff's_rule
    """

    def __init__(self, board_size, brute_force=False, run_time_checks=False, min_negative_path_len=2,
                 negative_outcome_nodes_max_cache_size_bytes=1024 * 1024 * 1024):
        self.board_size = board_size
        self.found_walks_count = 0
        self.brute_force = brute_force
        self.algo_start_time = time.time()
        self.negative_outcome_nodes_max_cache_size_bytes = negative_outcome_nodes_max_cache_size_bytes
        self.negative_outcome_nodes_cache = cachetools.LRUCache(maxsize=self.negative_outcome_nodes_max_cache_size_bytes)
        self.generated_paths_set = set()
        self.run_time_checks = run_time_checks
        self.min_negative_path_len = min_negative_path_len
        # self.positive_outcome_nodes_cache = LRUCache(1000000)

    def init_internal_data(self):
        self.algo_start_time = time.time()
        self.found_walks_count = 0
        self.find_possible_moves_helper.cache_clear()

    def negative_outcomes_cache_size(self):
        return self.negative_outcome_nodes_cache.currsize

    @staticmethod
    def seconds_to_str(t):
        return "%02d:%02d:%02d.%03d" % \
               functools.reduce(lambda ll, b: divmod(ll[0], b) + ll[1:],
                                [(round(t * 1000),), 1000, 60, 60])

    @staticmethod
    def drop_out_moves_in_path(moves, path):
        current_path_set = set(path)
        res = [_ for _ in moves if _ not in current_path_set]
        return res

    @functools.cache
    def find_possible_moves_helper(self, node):
        possible_moves = []
        # Find all new possible moves by the rules for moving a Knight figure on the Chess desk from a given square.
        # There are at most 8 possible moves from the current square. Of course we must check if a possible move is not
        # out of the desk and is not already in the current path in consideration,
        # for which I am using a set for faster search in the current path.

        new_node = (node[0] + 1, node[1] + 2)
        if new_node[0] < self.board_size and new_node[1] < self.board_size:
            possible_moves.append(new_node)

        new_node = (node[0] + 1, node[1] - 2)
        if new_node[0] < self.board_size and new_node[1] >= 0:
            possible_moves.append(new_node)

        new_node = (node[0] + 2, node[1] + 1)
        if new_node[0] < self.board_size and new_node[1] < self.board_size:
            possible_moves.append(new_node)

        new_node = (node[0] + 2, node[1] - 1)
        if new_node[0] < self.board_size and new_node[1] >= 0:
            possible_moves.append(new_node)

        new_node = (node[0] - 2, node[1] + 1)
        if new_node[0] >= 0 and new_node[1] < self.board_size:
            possible_moves.append(new_node)

        new_node = (node[0] - 2, node[1] - 1)
        if new_node[0] >= 0 and new_node[1] >= 0:
            possible_moves.append(new_node)

        new_node = (node[0] - 1, node[1] + 2)
        if new_node[0] >= 0 and new_node[1] < self.board_size:
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
        walk_string = "b{}{}".format(ascii_lc[node[0]], node[1] + 1)
        for node in walk[1:]:
            walk_string = "{}{}{}".format(walk_string, ascii_lc[node[0]], node[1] + 1)
        return walk_string

    def clear_bit(self, value, bit):
        return value & ~(1 << bit)

    def set_bits(self, value, bits):
        for bit in bits:
            value |= (1 << bit)
        return value

    def make_node_mtx_ctx(self, path):
        mtx_ctx = 0
        b = [(path_node[1] * self.board_size + path_node[0]) for path_node in path]
        mtx_ctx = self.set_bits(mtx_ctx, b)
        # print("{:08b}, {}".format(mtx_ctx, mtx_ctx))
        return mtx_ctx

    def check_negative_node_previous_node_pms_and_cache(self, path):
        while len(path) >= self.min_negative_path_len:
            node = path[-1]
            path = path[:-1]
            pms = self.find_possible_moves(path[-1], path)
            if not pms:
                path.append(node)
                raise (RuntimeError(f"Previous node in the path doesn't have possible moves! What is wrong?!?!?!?! Path:"
                                    f"{path}"))
            if len(pms) > 1:
                break
            self.add_to_negative_outcome_nodes_cache(path)
        self.add_to_negative_outcome_nodes_cache(path)

    def check_positive_node_previous_node_pms_and_cache(self, path):
        end_segment = []

        while len(path) >= 2:
            end_segment.append(path[-1])
            path = path[:-1]
            pms = self.find_possible_moves(end_segment[-1], path)
            if len(pms) > 1:
                break

        if len(end_segment) > 0:
            self.add_to_positive_outcome_nodes_cache(path, end_segment)

    def add_to_negative_outcome_nodes_cache(self, dead_end_path):
        mtx_ctx = self.compute_mtx_ctx(dead_end_path)
        mtx_ctx_count = self.negative_outcome_nodes_cache.get(mtx_ctx)
        if mtx_ctx_count:
            mtx_ctx_count += 1
            self.negative_outcome_nodes_cache[mtx_ctx] = mtx_ctx_count
        else:
            self.negative_outcome_nodes_cache[mtx_ctx] = 1

    def add_to_positive_outcome_nodes_cache(self, path, end_segment):
        logging.debug("add_to_positive_outcome_nodes_cache -> path: {}".format(path))
        mtx_ctx = self.compute_mtx_ctx(path)
        val = self.positive_outcome_nodes_cache.get(mtx_ctx)
        if val == -1:
            self.positive_outcome_nodes_cache.put(mtx_ctx, [end_segment])
        else:
            val.append(end_segment)
            self.positive_outcome_nodes_cache.put(mtx_ctx, val)

    def compute_mtx_ctx(self, path):
        mtx_ctx = self.make_node_mtx_ctx(path)
        return mtx_ctx

    def check_path(self, path):
        node = path[0]
        pms = self.find_possible_moves(node, path[0:1])
        for next_node_idx in range(1, len(path)):
            next_node = path[next_node_idx]
            pms = self.find_possible_moves(node, path[0:next_node_idx])
            if next_node not in pms:
                raise RuntimeError("Incorrect path! path: {}, node: {}, pms: {}, next_node: {}"
                                   .format(path), pms, next_node)
            node = next_node
        if tuple(path) in self.generated_paths_set:
            raise RuntimeError("Path is already generated! WTF?!?!?!?! path: {}, node: {}, pms: {}, next_node: {}"
                               .format(path), pms, next_node)
        self.generated_paths_set.add(tuple(path))

    def check_if_path_found(self, new_path):
        if len(new_path) == self.board_size * self.board_size:
            if self.run_time_checks:
                try:
                    self.check_path(new_path)
                except RuntimeError as rte:
                    logging.error(rte)

            self.found_walks_count += 1
            # self.check_positive_node_previous_node_pms_and_cache(new_path)
            if self.found_walks_count % 50 == 0:  # (3 ** self.board_size) == 0:
                tt = time.time() - self.algo_start_time
                logging.info("Current self.negative_outcome_nodes_cache size: {}, size in bytes {}".format(
                    len(self.negative_outcome_nodes_cache),
                    self.negative_outcome_nodes_cache.currsize))
                # logging.info("Current self.positive_outcome_nodes_cache hits: {}, misses: {}, size: {}".format(
                #     self.positive_outcome_nodes_cache.cache_hits,
                #     self.positive_outcome_nodes_cache.cache_misses,
                #     self.positive_outcome_nodes_cache.size()))

            logging.info("#{} {}".format(self.found_walks_count, self.make_walk_path_string(new_path)))
            return True
        return False

    def find_new_pms_and_dead_ends(self, new_path, current_new_paths_pms):
        new_pms = self.find_possible_moves(new_path[-1], new_path)
        to_filter = []

        # Filter negative outcome paths
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
        current_dead_end_paths = []

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
            # Skip nodes with more possible outcomes than the first node in the sorted list
            if not self.brute_force and len(new_path_possible_moves[1]) > cur_min_pms_len:
                break

            current_path.append(new_path_possible_moves[0])
            self.find_walks(current_path, new_path_possible_moves[1])
            current_path.pop()

    def print_all_walks_info(self):
        logging.info("Total # of possible walks found: {}".format(self.found_walks_count))
        logging.info(
            "self.find_possible_moves_helper Cache Info: {}".format(self.find_possible_moves_helper.cache_info()))
        logging.info("Final self.negative_outcome_nodes_cache size: {}, size in bytes: {}".format(
            len(self.negative_outcome_nodes_cache),
            self.negative_outcome_nodes_cache.currsize))
        # logging.info("Final self.positive_outcome_nodes_cache hits: {}, misses: {}, size: {}".format(
        #     self.positive_outcome_nodes_cache.cache_hits,
        #     self.positive_outcome_nodes_cache.cache_misses,
        #     self.positive_outcome_nodes_cache.size()))

    def bootstrap_search(self):
        possible_moves = []

        for x_coord in range(self.board_size):
            for y_coord in range(self.board_size):
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
        logging.info("*** ALGO PARAMETERS START ***")
        logging.info("Board size: {}x{}".format(self.board_size, self.board_size))
        logging.info("Brute force: {}".format(self.brute_force))
        logging.info("Run time checks: {}".format(self.run_time_checks))
        logging.info("Min negative path len: {}".format(self.min_negative_path_len))
        logging.info("Negative outcome nodes max cache size bytes: {}".format(self.negative_outcome_nodes_max_cache_size_bytes))
        logging.info("*** ALGO PARAMETERS END ***")
        logging.info("Clearing Cache")
        # self.find_possible_moves_helper.cache_clear()
        # self.set_bit.cache_clear()
        self.bootstrap_search()

        tt = time.time() - self.algo_start_time
        self.print_all_walks_info()
        logging.info("*** ALGO TOTAL TIME: {}s ***".format(self.seconds_to_str(tt)))
        logging.info("*** ALGO END ***".format())
        return tt, tt / self.found_walks_count


def main():
    runtimes = []
    runtimes_per_path = []

    for _ in range(2):
        kta = KnightsTourAlgo(6, brute_force=True, run_time_checks=False, min_negative_path_len=2,
                              negative_outcome_nodes_max_cache_size_bytes=5 * 1000 * 1000)
        rt, rt_path = kta.run()
        runtimes.append(rt)
        runtimes_per_path.append(rt_path)

    logging.info("Avg. runtime after #{} runs is: {}, avg. runtime per path is: {}. "
                 .format(len(runtimes),
                         kta.seconds_to_str(statistics.mean(runtimes)),
                         statistics.mean(runtimes_per_path)))


if __name__ == '__main__':
    main()
