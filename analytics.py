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
    logging.info(f"Calculating mean of {meanColumn}")
    average = df[meanColumn].mean()
    return average

def median(df, medianColumn):
    logging.info(f"Calculating median of {medianColumn}")
    median_ = df[medianColumn].median()
    return median_

def mode(df , modeColumn):
    logging.info(f"Calculating mode of {modeColumn}")
    mode_ = df[modeColumn].mode()
    return mode_

def minimum(df, minColumn):
    logging.info(f"Calculating the minimum of {minColumn}")
    minimum_ = df[minColumn].min()
    return minimum_

def maximum(df, maxColumn):
    logging.info(f"Calculating the maximum of {maxColumn}")
    maximum_ = df[maxColumn].max()
    return maximum_

def standardDeviation(df, stdColumn):
    logging.info(f"Calculating the standard deviation of {stdColumn}")
    std_dev = df[stdColumn].std()
    return std_dev