import argparse
import sys
import copy
import yaml
import os.path

DEFAULT_CONFIG_PATH = "config.yaml"
DEFAULT_LOG_FORMAT_STRING = "%(asctime)s %(levelname)s %(message)s"

EMAILS_SMALL_DATASET = "emails_small"
COUNT_VECTORIZER = "CountVectorizer"
MULTINOMIAL_NB_CLASSIFIER = "MultinomialNB"

LOGGING_OUTPUT_SYSLOG = "syslog"
LOGGING_FORMAT_JSON = "json"

DEFAULT_CONFIG = {
    "Learning": {
        "dataset": EMAILS_SMALL_DATASET,
        "vectorizer": COUNT_VECTORIZER,
        "classifier": MULTINOMIAL_NB_CLASSIFIER,
        "dataset_path": "emails.csv",
        "filter_path": "spam_filter"
    },
    "RabbitMQ": {
        "connstring": "amqp://guest:guest@127.0.0.1",
        "queue_name": "spam",
    },
    "Filtering": {
        "batchsize": 10,
    },
    "Alert": {
        "url": "http://localhost/en/api/2.0/users/suss/",
        "token": "29925f9f740a5567b70325419f420a9723b5b37d",
    },
    "Logging": {
        "output": "console",
        "format": "text",
        "format_string": DEFAULT_LOG_FORMAT_STRING,
        "level": "info",
        "syslog_address": "/var/run/syslog",
        "syslog_facility": "local1",
    },
}

flag2config = {
    "learning_dataset": ("Learning", "dataset"),
    "learning_vectorizer": ("Learning", "vectorizer"),
    "learning_dataset_path": ("Learning", "dataset_path"),
    "learning_filter_path": ("Learning", "filter_path"),
    "learning_classifier": ("Learning", "classifier"),
    "rabbitmq_connstring": ("RabbitMQ", "connstring"),
    "rabbitmq_queue_name": ("RabbitMQ", "queue_name"),
    "filtering_batchsize": ("Filtering", "batchsize"),
    "alert_url": ("Alert", "url"),
    "alert_token": ("Alert", "token"),
    "logging_output": ("Logging", "output"),
    "logging_format": ("Logging", "format"),
    "logging_format_string": ("Logging", "format_string"),
    "logging_level": ("Logging", "level"),
    "logging_syslog_address": ("Logging", "syslog_address"),
    "logging_syslog_facility": ("Logging", "syslog_facility"),
}

typed_fields = {
    ("Filtering", "batchsize"): int
}

Config = copy.deepcopy(DEFAULT_CONFIG)


def parse_arguments():
    '''
    Parse flags
    '''
    parser = argparse.ArgumentParser(
        description="Reckognize spam and alert about it")
    parser.add_argument("--learning_dataset", action="store",
                        default=DEFAULT_CONFIG["Learning"]["dataset"],
                        help="dataset to learn from")
    parser.add_argument("--learning_vectorizer", action="store",
                        default=DEFAULT_CONFIG["Learning"]["vectorizer"],
                        help="vectorizer to vectorize messages text")
    parser.add_argument("--learning_classifier", action="store",
                        default=DEFAULT_CONFIG["Learning"]["classifier"],
                        help="classifier to predict if msg is spam or not")
    parser.add_argument("--learning_dataset_path", action="store",
                        default=DEFAULT_CONFIG["Learning"]["dataset_path"],
                        help="path to dataset file/folder to learn from")
    parser.add_argument("--learning_filter_path", action="store",
                        default=DEFAULT_CONFIG["Learning"]["filter_path"],
                        help="path to store learned filter")
    parser.add_argument("--filtering_batchsize", action="store", type=int,
                        default=DEFAULT_CONFIG["Filtering"]["batchsize"],
                        help="Batch size for messages from queue to filter")
    parser.add_argument("--alert_url", action="store",
                        default=DEFAULT_CONFIG["Alert"]["url"],
                        help="url to alert about spam message")
    parser.add_argument("--alert_token", action="store",
                        default=DEFAULT_CONFIG["Alert"]["token"],
                        help="auth token to alert about spam message")
    parser.add_argument("--rabbitmq_connstring", action="store",
                        default=DEFAULT_CONFIG["RabbitMQ"]["connstring"],
                        help="url to alert about spam message")
    parser.add_argument("--rabbitmq_queue_name", action="store",
                        default=DEFAULT_CONFIG["RabbitMQ"]["queue_name"],
                        help="queue name with messages for spam filtering")
    parser.add_argument("--logging_output", action="store",
                        default=DEFAULT_CONFIG["Logging"]["output"],
                        help="logging output, could be (console, syslog)")
    parser.add_argument("--logging_format", action="store",
                        default=DEFAULT_CONFIG["Logging"]["format"],
                        help="logging format, could be (json, text)")
    parser.add_argument("--logging_format_string", action="store",
                        default=DEFAULT_CONFIG["Logging"]["format_string"],
                        help="logging format string, would be applied if text format specified")
    parser.add_argument("--logging_level", action="store",
                        default=DEFAULT_CONFIG["Logging"]["level"],
                        help="logging level")
    parser.add_argument("--logging_syslog_address", action="store",
                        default=DEFAULT_CONFIG["Logging"]["syslog_address"],
                        help="syslog address, would be applied if output is syslog")
    parser.add_argument("--logging_syslog_facility", action="store",
                        default=DEFAULT_CONFIG["Logging"]["syslog_facility"],
                        help="syslog facility, would be applied if output is syslog")

    parser.add_argument("--learn", action="store_true",
                        help="perform learning process, dump a matrix")
    parser.add_argument("--save_config", action="store_true",
                        help="save current config")
    parser.add_argument("--config_path", action="store",
                        default=DEFAULT_CONFIG_PATH,  help="path to config")
    return parser.parse_args()


def apply_config_from_file(config):
    '''
    Write values from config file to global config
    '''
    for section in config:
        if section in Config:
            for option in config[section]:
                if option in Config[section]:
                    if (section, option) in typed_fields:
                        fn = typed_fields[(section, option)]
                        Config[section][option] = fn(config[section][option])
                    else:
                        Config[section][option] = config[section][option]
    return config


def apply_flags(args):
    '''
    Write values from flags to global config
    '''
    for flag, entry in flag2config.items():
        if getattr(args, flag) != DEFAULT_CONFIG[entry[0]][entry[1]]:
            Config[entry[0]][entry[1]] = getattr(args, flag)


def write_config(path):
    '''
    Write global config to specified path
    '''
    with open(path, 'w') as stream:
        yaml.dump(Config, stream)


def init():
    args = parse_arguments()
    if args.save_config:
        write_config(args.config_path)
        print(f"config saved at {args.config_path}")
        sys.exit(0)
    config = None
    if os.path.isfile(args.config_path):
        with open(args.config_path, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as e:
                print("error parsing config file:", e)
                print("config from the flags will be used.")
            else:
                apply_config_from_file(config)
    else:
        print(f"cant find config at {args.config_path}, config from the flags will be used")
    apply_flags(args)
    return args
