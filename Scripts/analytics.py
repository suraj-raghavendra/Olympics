import requests
import pandas as pd
import logging


FIREBASE_URL = "https://olympics-62c6a-default-rtdb.firebaseio.com/"
# FIREBASE_URL = "https://suraj-test-45b9c-default-rtdb.firebaseio.com/"
NAMENODE = FIREBASE_URL + "NameNode/root/"
DATANODE = FIREBASE_URL + "DataNode/root/"
CURRENT_DIR = "root"
FILENAME = FIREBASE_URL + "/FileName"

FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


def mean(df, meanColumn):

    average = df[meanColumn].mean()
    return average
