import sys
import pickle
import os.path

import config
import learn
import server


if __name__ == "__main__":
    args = config.init()
    spam_filter = None
    spam_filter_path = config.Config["Learning"]["filter_path"]
    if os.path.isfile(spam_filter_path):
        try:
            with open(spam_filter_path, 'rb') as f:
                spam_filter = pickle.load(f)
        except Exception as e:
            print("Problem getting filter from file:", e)
    else:
        learn_cfg = config.Config["Learning"]
        learn_fabric = learn.LearnFabric(
            vectorizer_name=learn_cfg["vectorizer"],
            classifier_name=learn_cfg["classifier"])
        spam_filter = learn.SpamFilter(classifier=learn_fabric.classifier,
                                       vectorizer=learn_fabric.vectorizer)
        dataset_loader = learn.DataLoaderFactory(
            name=learn_cfg["dataset"],
            path=learn_cfg["dataset_path"],
        ).data_loader
        spam_filter.learn(dataset_loader)
        if args.learn:
            with open(spam_filter_path, 'wb') as f:
                pickle.dump(spam_filter, f)
            print("learning finished")
            sys.exit(0)
    rabbitmq_cfg = config.Config["RabbitMQ"]
    server.start(spam_filter=spam_filter,
                 connstring=rabbitmq_cfg["connstring"],
                 queue_name=rabbitmq_cfg["queue_name"],
                 batch_size=config.Config["Filtering"]["batchsize"])
    sys.exit(0)
