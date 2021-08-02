import statistics
import logging
from knightstour import KnightsTourAlgo
from dynaconf import settings


def config_logging():
    log_filename = settings.LOG_FILENAME or __file__
    log_filename += ".{0}x{0}.log".format(settings.BOARD_SIZE)
    logging.basicConfig(filename=log_filename,
                        filemode=settings.LOG_FILE_MODE,
                        format='%(asctime)s %(pathname)s:%(lineno)d %(levelname)s: %(message)s')
    print("Logging configured. Log filename is: {}".format(log_filename))

def main():
    config_logging()
    runtimes = []
    runtimes_per_path = []
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.info("*** TEST START***")
    for run_number in range(settings.N_RUNS):
        logger.info("*** TEST RUN # {} ***".format(run_number + 1))
        kta = KnightsTourAlgo(board_size=settings.BOARD_SIZE,
                              brute_force=settings.BRUTE_FORCE,
                              run_time_checks=settings.RUN_TIME_CHECKS,
                              enable_cache=settings.ENABLE_CACHE,
                              min_negative_path_len=settings.MIN_NEG_PATH_LENGTH,
                              negative_outcome_nodes_max_cache_size=settings.NEG_OUTCOMES_CACHE_SIZE,
                              percent_to_evict=settings.PERCENT_TO_EVICT_FROM_NEG_NODES_CACHE,
                              redis_host=settings.REDIS_HOST,
                              redis_port=settings.REDIS_PORT,
                              redis_password=settings.REDIS_PASSWORD,
                              redis_set_key=settings.REDIS_SET_KEY,
                              redis_ev_list_key=settings.REDIS_EV_LIST_KEY,
                              redis_hits_key=settings.REDIS_HITS_KEY,
                              redis_misses_key=settings.REDIS_MISSES_KEY,
                              log_cache_info_timer_timeout=settings.LOG_CACHE_INFO_TIMER_TIMEOUT)
        rt, rt_path = kta.run()
        runtimes.append(rt)
        runtimes_per_path.append(rt_path)
    logger.info("=" * 100)
    logger.info("Avg. runtime after #{} runs is: {}, avg. runtime per path is: {}."
                 .format(len(runtimes),
                         kta.seconds_to_str(statistics.mean(runtimes)),
                         statistics.mean(runtimes_per_path)))
    logger.info("*** TEST END ***")


if __name__ == '__main__':
    main()
