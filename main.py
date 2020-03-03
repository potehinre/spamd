import sys
import pickle
import os.path
import logging
import logging.handlers
import syslog

import json_logging

import config
import learn
import server


logger = logging.getLogger('spamd')


def logging_level(level):
    str2level = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }
    if level in str2level:
        return str2level[level]
    else:
        print(f"Unknown logging level: {level}. Log level will be set to NOTSET")
        return logging.NOTSET


def logging_facility(facility):
    str2fac = {
        "kern": syslog.LOG_KERN,
        "user": syslog.LOG_USER,
        "mail": syslog.LOG_MAIL,
        "daemon": syslog.LOG_DAEMON,
        "auth": syslog.LOG_AUTH,
        "lpr": syslog.LOG_LPR,
        "news": syslog.LOG_NEWS,
        "uucp": syslog.LOG_UUCP,
        "cron": syslog.LOG_CRON,
        "syslog": syslog.LOG_SYSLOG,
        "local0": syslog.LOG_LOCAL0,
        "local1": syslog.LOG_LOCAL1,
        "local2": syslog.LOG_LOCAL2,
        "local3": syslog.LOG_LOCAL3,
        "local4": syslog.LOG_LOCAL4,
        "local5": syslog.LOG_LOCAL5,
        "local6": syslog.LOG_LOCAL6,
        "local7": syslog.LOG_LOCAL7
    }
    if facility in str2fac:
        return str2fac[facility]
    else:
        raise ValueError(f"uknown facility in config: {facility}")


def logging_init(level, output, _format, format_string, syslog_address, syslog_facility):
    logger.setLevel(logging_level(level))
    handler = None
    if output == config.LOGGING_OUTPUT_SYSLOG:
        facility = logging_facility(syslog_facility)
        handler = logging.handlers.SysLogHandler(address=syslog_address,
                                                 facility=facility)
    else:
        handler = logging.StreamHandler(sys.stdout)
    if _format == config.LOGGING_FORMAT_JSON:
        json_logging.ENABLE_JSON_LOGGING = True
        json_logging.init_non_web()
    else:
        handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(handler)


if __name__ == "__main__":
    args = config.init()
    logging_config = config.Config["Logging"]
    logging_init(level=logging_config["level"],
                 output=logging_config["output"],
                 _format=logging_config["format"],
                 format_string=logging_config["format_string"],
                 syslog_address=logging_config["syslog_address"],
                 syslog_facility=logging_config["syslog_facility"])
    spam_filter = None
    spam_filter_path = config.Config["Learning"]["filter_path"]
    if os.path.isfile(spam_filter_path):
        logger.info(f"loading spam filter from a file {spam_filter_path}")
        try:
            with open(spam_filter_path, 'rb') as f:
                spam_filter = pickle.load(f)
        except Exception as e:
            logger.error("can't load spam filter from a file {0}".format(e))
    else:
        logger.info("Haven't found file with spam filter. started learning process")
        learn_cfg = config.Config["Learning"]
        try:
            learn_fabric = learn.LearnFabric(
                vectorizer_name=learn_cfg["vectorizer"],
                classifier_name=learn_cfg["classifier"])
            spam_filter = learn.SpamFilter(classifier=learn_fabric.classifier,
                                           vectorizer=learn_fabric.vectorizer)
        except Exception as e:
            logger.error("Spam filter init error: {0}".format(e), exc_info=True)
            raise
        try:
            dataset_loader = learn.DataLoaderFactory(
                name=learn_cfg["dataset"],
                path=learn_cfg["dataset_path"],
            ).data_loader
        except Exception as e:
            logger.error("can't load dataset for spam filter learning: {0}".format(e))
            raise
        spam_filter.learn(dataset_loader)
        if args.learn:
            logger.info(f"saving spam filter as {spam_filter_path}")
            try:
                with open(spam_filter_path, 'wb') as f:
                    pickle.dump(spam_filter, f)
            except Exception as e:
                logger.error("can't save spam filter: {0}".format(e))
                raise
            sys.exit(0)
    rabbitmq_cfg = config.Config["RabbitMQ"]
    logger.info("listening for messages to filter...")
    try:
        server.start(spam_filter=spam_filter,
                    connstring=rabbitmq_cfg["connstring"],
                    queue_name=rabbitmq_cfg["queue_name"],
                    batch_size=config.Config["Filtering"]["batchsize"])
    except Exception as e:
        logger.error("error during listening the queue: {0}".format(e))
        raise
