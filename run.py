import sys
import yaml
import json
import argparse

from munch import DefaultMunch

from logger.logging import setup_logging
from engine import Engine

import logging


def run(cfg):
    logging.basicConfig(level=logging.DEBUG)
    engine = Engine(cfg)
    engine.run()


def get_config():
    parser = argparse.ArgumentParser(description='Trading Engine named Strela')
    parser.add_argument('-config', help='a path to configuration file')

    args = parser.parse_args()
    filename = args.config

    with open(filename, 'r') as ymlfile:
        cfg_dict = yaml.load(ymlfile)
        return DefaultMunch.fromDict(cfg_dict, None)


def start_engine():
    cfg = get_config()
    if cfg is None:
        raise Exception("Unable to connect to config")

    try:
        logger_name = cfg.logger.name
    except Exception:
        logger_name = None
        logger_name = "mm_bot"

    logger = setup_logging(cfg, logger_name)
    logger.info("Configurations: %s", json.dumps(cfg))
    run(cfg)


if __name__ == "__main__":
    start_engine()

