import statistics
import logging
from knightstour import KnightsTourAlgo
import json


CONFIG_DEFAULT_NAME = "config.json"


def config_logging(json_conf):
    logging.basicConfig(filename=__file__ + ".{0}x{0}.log".format(json_conf["BOARD_SIZE"]),
                        filemode="a+",
                        # encoding='utf-8',
                        format='[%(asctime)s] [%(levelname)s]: %(message)s',
                        level=logging.DEBUG)


def load_config(config_name=CONFIG_DEFAULT_NAME):
    with open(config_name) as f:
        config_json_obj = json.load(f)
    return config_json_obj


def main():
    json_conf = load_config()
    config_logging(json_conf)
    logging.info("[Loaded config file] - > {} ".format(CONFIG_DEFAULT_NAME))
    logging.info("[Logging configured]")

    runtimes = []
    runtimes_per_path = []

    logging.info("[*** TEST START***]")
    for run_number in range(json_conf["N_RUNS"]):
        logging.info("[*** TEST RUN # {} ***]".format(run_number + 1))
        kta = KnightsTourAlgo(json_conf["BOARD_SIZE"], brute_force=json_conf["BRUTE_FORCE"], run_time_checks=json_conf["RUN_TIME_CHECKS"],
                              min_negative_path_len=json_conf["MIN_NEG_PATH_LEN"], negative_outcome_nodes_max_cache=json_conf["NEG_OUTCOMES_CACHE"],
                              percent_to_evict=json_conf["PERCENT_TO_EVICT"])
        rt, rt_path = kta.run()
        runtimes.append(rt)
        runtimes_per_path.append(rt_path)

    logging.info("=" * 100)
    logging.info("[Avg. runtime after #{} runs is: {}, avg. runtime per path is: {}. ]"
                 .format(len(runtimes),
                         kta.seconds_to_str(statistics.mean(runtimes)),
                         statistics.mean(runtimes_per_path)))
    logging.info("[*** TEST END ***]")


if __name__ == '__main__':
    main()
