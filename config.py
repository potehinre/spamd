import argparse
import sys
import copy
import yaml
import os.path

DEFAULT_CONFIG_PATH = "config.yaml"

EMAILS_SMALL_DATASET = "emails_small"
COUNT_VECTORIZER = "CountVectorizer"
MULTINOMIAL_NB_CLASSIFIER = "MultinomialNB"

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
        "batchsize": 100,
    },
    "Alert": {
        "url": "http://127.0.0.1:8000/spam",
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
    parser.add_argument("--rabbitmq_connstring", action="store",
                        default=DEFAULT_CONFIG["RabbitMQ"]["connstring"],
                        help="url to alert about spam message")
    parser.add_argument("--rabbitmq_queue_name", action="store",
                        default=DEFAULT_CONFIG["RabbitMQ"]["queue_name"],
                        help="queue name with messages for spam filtering")

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
    config = None
    if os.path.isfile(args.config_path):
        with open(args.config_path, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as e:
                print("Exc happened while reading config file:", e)
            else:
                apply_config_from_file(config)
    apply_flags(args)
    if args.save_config:
        write_config(args.config_path)
        sys.exit(0)
    return args
