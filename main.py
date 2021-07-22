import statistics
import logging
from knightstour import KnightsTourAlgo
from knightstour import FIFOSet


logging.basicConfig(filename=__file__ + ".log",
                    filemode="a+",
                    # encoding='utf-8',
                    format='[%(asctime)s] [%(levelname)s]: %(message)s',
                    level=logging.DEBUG)


def main():
    runtimes = []
    runtimes_per_path = []

    logging.info("*** TEST START***")
    for run_number in range(10):
        logging.info("*** TEST RUN # {} ***".format(run_number + 1))
        kta = KnightsTourAlgo(5, brute_force=True, run_time_checks=False, min_negative_path_len=2,
                              negative_outcome_nodes_max_cache_size_bytes=10 * 1000 * 1000)
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
