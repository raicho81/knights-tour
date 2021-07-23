import statistics
import logging
from knightstour import KnightsTourAlgo


BOARD_SIZE = 6
N_RUNS = 2
NEG_OUTCOMES_CACHE = 50 * 1000 * 1000
PERCENT_TO_EVICT = 5
MIN_NEG_PATH_LEN = 2
RUN_TIME_CHECKS = False
BRUTE_FORCE = True


logging.basicConfig(filename=__file__ + ".{0}x{0}.log".format(BOARD_SIZE),
                    filemode="a+",
                    # encoding='utf-8',
                    format='[%(asctime)s] [%(levelname)s]: %(message)s',
                    level=logging.DEBUG)


def main():
    runtimes = []
    runtimes_per_path = []

    logging.info("*** TEST START***")
    for run_number in range(N_RUNS):
        logging.info("*** TEST RUN # {} ***".format(run_number + 1))
        kta = KnightsTourAlgo(BOARD_SIZE, brute_force=BRUTE_FORCE, run_time_checks=RUN_TIME_CHECKS, min_negative_path_len=MIN_NEG_PATH_LEN,
                              negative_outcome_nodes_max_cache=NEG_OUTCOMES_CACHE, percent_to_evict=PERCENT_TO_EVICT)
        rt, rt_path = kta.run()
        runtimes.append(rt)
        runtimes_per_path.append(rt_path)

    logging.info("=" * 100)
    logging.info("Avg. runtime after #{} runs is: {}, avg. runtime per path is: {}. "
                 .format(len(runtimes),
                         kta.seconds_to_str(statistics.mean(runtimes)),
                         statistics.mean(runtimes_per_path)))
    logging.info("*** TEST END ***")


if __name__ == '__main__':
    main()
