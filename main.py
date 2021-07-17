import functools
import statistics
from string import ascii_lowercase as ascii_lc
import time
import logging
from lru_cache import LRUCache


logging.basicConfig(filename=__file__ + ".log",
                    filemode="a+",
                    # encoding='utf-8',
                    format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class KnightsTourAlgo:
    def __init__(self, board_size, brute_force):
        self.board_size = board_size
        self.found_walks_count = 0
        self.brute_force = brute_force
        self.start_time = time.time()
        self.negative_outcome_nodes_cache = set()   # LRUCache(10000000)

    def negative_outcomes_cache_size(self):
        return len(self.negative_outcome_nodes_cache)
        # return self.negative_outcome_nodes_cache.size()

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
        walk_line = "b{}{}".format(ascii_lc[node[0]], node[1] + 1)
        for node in walk[1:]:
            walk_line = "{}{}{}".format(walk_line, ascii_lc[node[0]], node[1] + 1)
        return walk_line

    def clear_bit(self, value, bit):
        return value & ~(1 << bit)

    # @functools.lru_cache(1000*20)
    def set_bits(self, value, bits):
        for bit in bits:
            value |= (1 << bit)
        return value

    def make_node_vicinity_context_matrix(self, path):
        node = path[-1]
        x, y = node
        widen_with = self.board_size // 2
        minx = x - widen_with
        maxx = x + widen_with
        miny = y - widen_with
        maxy = y + widen_with
        vicinity_context_matrix = 0

        b = [path_node[1] * (maxy - miny + self.board_size % 2) + path_node[0] for path_node in path] #if path_node[0] >= miny and path_node[0] <= maxy and path_node[1] >= minx and path_node[1] <= maxx
        vicinity_context_matrix = self.set_bits(vicinity_context_matrix, b)
        # print("{:08b}, {}".format(vicinity_context_matrix, vicinity_context_matrix))
        return vicinity_context_matrix

    def check_previous_node_pms_and_cache(self, path):
        while len(path) >= 2:
            node = path[-1]
            path = path[:-1]
            pms = self.find_possible_moves(node, path)
            if len(pms) > 1:
                path.append(node)
                break
        self.add_to_negative_outcome_nodes_cache(path)

    def add_to_negative_outcome_nodes_cache(self, dead_end_path):
        vicinity_mtx_ctx = self.compute_vicinity_mtx_ctx(dead_end_path)
        if vicinity_mtx_ctx not in self.negative_outcome_nodes_cache:
            self.negative_outcome_nodes_cache.add(vicinity_mtx_ctx)
        # val = self.negative_outcome_nodes_cache.get(vicinity_mtx_ctx)
        # if val == -1:
        #     self.negative_outcome_nodes_cache.put(vicinity_mtx_ctx, 1)
        # else:
        #     val += 1
        #     self.negative_outcome_nodes_cache.put(vicinity_mtx_ctx, val)

    def compute_vicinity_mtx_ctx(self, path):
        vicinity_mtx_ctx = self.make_node_vicinity_context_matrix(path)
        return vicinity_mtx_ctx

    def find_walks(self, current_path, possible_moves):
        current_new_paths_pms = []
        current_dead_end_paths = []

        for possible_move in possible_moves:
            new_path = current_path.copy()
            new_path.append(possible_move)

            if len(new_path) == self.board_size * self.board_size:
                self.found_walks_count += 1
                if self.found_walks_count % 50 == 0:     #(3 ** self.board_size) == 0:
                    tt = time.time() - self.start_time
                    logging.info("Current dead end nodes cache size: {}".format(
                        len(self.negative_outcome_nodes_cache)))
                    # logging.info("Current dead end nodes cache hits: {}, misses: {}, size: {}, capacity: {}".format(
                    #     self.negative_outcome_nodes_cache.size(), self.negative_outcome_nodes_cache.cache_hits,
                    #     self.negative_outcome_nodes_cache.cache_misses,
                    #     self.negative_outcome_nodes_cache.capacity))

                logging.info("#{} {}".format(self.found_walks_count, self.make_walk_path_string(new_path)))
                continue

            new_pms = self.find_possible_moves(new_path[-1], new_path)

            # Calculate new pms weights and organize the pms in a new structure and sort them by this weights
            if new_pms:
                if len(new_path) > self.board_size:
                    for new_pm_node in new_pms:
                        new_path.append(new_pm_node)
                        v_mtx_ctx = self.compute_vicinity_mtx_ctx(new_path)
                        if v_mtx_ctx in self.negative_outcome_nodes_cache:
                            new_pms.remove(new_pm_node)
                        new_path.pop()
                if not new_pms:
                    continue

            if new_pms:
                current_new_paths_pms.append((new_path, len(new_pms), new_pms))
            else:
                current_dead_end_paths.append(new_path)

        if not current_new_paths_pms:
            for p in current_dead_end_paths:
                self.check_previous_node_pms_and_cache(p)
            return

            # We take possible moves with their contexts if they exist or the last node in the current_path and
            # its context and put them into self.negative_outcome_nodes_cache dict for later use. I plan to use later XOR to compare
            # vicinity_context_matrix objects and calculate a penalty metric for the nodes who have entered the same or similar vicinity_matrix_context
            # and then drop out the non-perspective nodes when computing new possible moves.
            # I guess I can also go backwards and chech if the parent node in the path has any other possible moves becuse if it doesn't then it is also a
            # "dead" node, which must be avoided and added to the self.negative_outcome_nodes_cache with its context for later use. I guess this
            # task - "going back" will require some changes in the code but really nothing so special. I guess I can track back as many as possible "bad" nodes
            # and cache their current vicinity context

        if not self.brute_force:
            current_new_paths_pms = sorted(current_new_paths_pms, key=lambda x: x[1])
            if current_new_paths_pms:
                cur_min_pms_len = current_new_paths_pms[0][1]

        for path_possible_moves in current_new_paths_pms:
            # Skip nodes with more possible outcomes than the first node in the sorted list
            if not self.brute_force and path_possible_moves[1] > cur_min_pms_len:
                break

            new_path = path_possible_moves[0]
            self.find_walks(new_path, path_possible_moves[2])

    def print_all_walks_info(self):
        logging.info("Total # of possible walks found: {}".format(self.found_walks_count))
        logging.info(
            "self.find_possible_moves_helper Cache Info: {}".format(self.find_possible_moves_helper.cache_info()))
        logging.info("Final dead end nodes cache size: {}".format(
            len(self.negative_outcome_nodes_cache)))
        # logging.info("Final dead end nodes cache hits: {}, misses: {}, size: {}, capacity: {}".format(
        #     self.negative_outcome_nodes_cache.size(), self.negative_outcome_nodes_cache.cache_hits,
        #     self.negative_outcome_nodes_cache.cache_misses,
        #     self.negative_outcome_nodes_cache.capacity))
        # logging.info("Current set_bit cache Info: {}".format(self.set_bit.cache_info()))

    def run(self):
        logging.info("*** ALGO PARAMETERS START ***")
        logging.info("Board size: {}x{}".format(self.board_size, self.board_size))
        logging.info("Brute force: {}".format(self.brute_force))
        logging.info("*** ALGO PARAMETERS END ***")
        logging.info("Clearing Cache")
        self.find_possible_moves_helper.cache_clear()
        # self.set_bit.cache_clear()
        possible_moves = []

        for x_coord in range(self.board_size):
            for y_coord in range(self.board_size):
                start_node = (x_coord, y_coord)
                start_path = [start_node]
                pms = self.find_possible_moves(start_node, start_path)
                possible_moves.append((start_path, len(pms), pms))

        if not self.brute_force:
            possible_moves = sorted(possible_moves, key=lambda x: x[1])
            possible_move_min_len = possible_moves[0][1]

        for pm in possible_moves:
            if not self.brute_force and pm[1] > possible_move_min_len:
                break
            self.find_walks(pm[0], pm[2])

        tt = time.time() - self.start_time
        self.print_all_walks_info()
        logging.info("*** ALGO TOTAL TIME: {}s ***".format(self.seconds_to_str(tt)))
        logging.info("*** ALGO END ***".format())
        return tt


if __name__ == '__main__':
    runtimes = []
    for _ in range(5):
        kta = KnightsTourAlgo(5, True)
        runtimes.append(kta.run())

    logging.info("Avg. runtime after #{} runs is: {}s, STDDEV is: {:.3}s".format(len(runtimes),
                                                                                 kta.seconds_to_str(
                                                                                     statistics.mean(runtimes)),
                                                                                 statistics.stdev(runtimes)))
