import pandas as pd

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score

import config
import logging

logger = logging.getLogger("spamd.learn")


class DataLoaderFactory():
    def __init__(self, name, path):
        self.name = name
        self.path = path

    @property
    def data_loader(self):
        if self.name == config.EMAILS_SMALL_DATASET:
            return EmailsSmallDataLoader(self.path)
        elif self.name == config.SMS_DATASET:
            return SMSDataLoader(self.path)
        else:
            raise ValueError(f"Unknown data loader: {self.name}")


class EmailsSmallDataLoader():
    def __init__(self, path):
        self.df = pd.read_csv(path)
        self.df.drop_duplicates(inplace=True)

    def get_texts(self):
        return self.df["text"]

    def get_marks(self):
        return self.df["spam"] == 1


class SMSDataLoader():
    def __init__(self, path):
        self.df = pd.read_csv(path, sep="\t", header=None)
        self.df.drop_duplicates(inplace=True)

    def get_texts(self):
        return self.df[1]

    def get_marks(self):
        return self.df[0] == 'spam'


class LearnFabric():
    def __init__(self, classifier_name, vectorizer_name):
        self.classifier_name = classifier_name
        self.vectorizer_name = vectorizer_name

    @property
    def classifier(self):
        if self.classifier_name == config.MULTINOMIAL_NB_CLASSIFIER:
            return MultinomialNB()
        else:
            raise ValueError(f"Unknown classifier: {self.classifier_name}")

    @property
    def vectorizer(self):
        if self.vectorizer_name == config.COUNT_VECTORIZER:
            return CountVectorizer()
        else:
            raise ValueError(f"Unknown vectorizer: {self.vectorizer_name}")


class SpamFilter():
    def __init__(self, vectorizer, classifier):
        self.classifier = classifier
        self.vectorizer = vectorizer

    def learn(self, data_loader):
        train_texts = data_loader.get_texts()
        train_marks = data_loader.get_marks()
        self.vectorizer.fit(train_texts)
        messages = self.vectorizer.transform(train_texts)
        X_train, X_test, y_train, y_test = train_test_split(messages, train_marks, random_state=0)
        self.classifier.fit(X_train, y_train)

        pred = self.classifier.predict(X_train)
        logger.info("filter accuracy on train set is {0}".format(accuracy_score(y_train, pred)))
        pred_test = self.classifier.predict(X_test)
        logger.info("filter accuracy on test set is {0}".format(accuracy_score(y_test, pred_test)))

    def is_spam(self, msg_texts):
        msg_vector = self.vectorizer.transform(msg_texts)
        return self.classifier.predict(msg_vector)
