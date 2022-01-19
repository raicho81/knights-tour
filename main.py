import statistics
import logging
from knightstour import KnightsTourAlgo
import json


CONFIG_DEFAULT_NAME = "default-config.json"


def config_logging(json_conf):
    log_filename = json_conf["log_filename"] or __file__
    log_filename += ".{}x{}.log".format(json_conf["board_size_x"], json_conf["board_size_y"])
    logging.basicConfig(filename=log_filename,
                        filemode=json_conf["log_file_mode"],
                        # encoding='utf-8',
                        format='[%(asctime)s %(levelname)s] %(message)s',
                        level=logging.DEBUG)
    print("[Logging configured. Log filename: {}]".format(log_filename))


def load_config(config_name=CONFIG_DEFAULT_NAME):
    with open(config_name) as f:
        config_json_str = f.read()
        print("[JSON Config: {}]".format(config_json_str))
        config_json_obj = json.loads(config_json_str)

    return config_json_obj


def main():
    json_conf = load_config()
    config_logging(json_conf)
    logging.info("[Loaded config file: {}]".format(CONFIG_DEFAULT_NAME))

    runtimes = []
    runtimes_per_path = []

    logging.info("[*** TEST START***]")
    for run_number in range(json_conf["n_runs"]):
        logging.info("[*** TEST RUN # {} ***]".format(run_number + 1))
        kta = KnightsTourAlgo(json_conf["board_size_x"], json_conf["board_size_y"], brute_force=json_conf["brute_force"], run_time_checks=json_conf["run_time_checks"],
                              min_negative_path_len=json_conf["min_neg_path_length"], negative_outcome_nodes_max_cache_size=json_conf["neg_outcomes_cache_size"],
                              percent_to_evict=json_conf["percent_to_evict_from_neg_nodes_cache"])
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
