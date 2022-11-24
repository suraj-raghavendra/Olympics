import requests
import json
import sys
import hash
import pandas as pd
import logging

FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

FIREBASE_URL = "https://olympics-62c6a-default-rtdb.firebaseio.com/"
# FIREBASE_URL = "https://suraj-test-45b9c-default-rtdb.firebaseio.com/"
NAMENODE = FIREBASE_URL + "NameNode/root/"
DATANODE = FIREBASE_URL + "DataNode/root/"
CURRENT_DIR = "root"
FILENAME = FIREBASE_URL + "/FileName"

def mapPartition(p):
    # PARTITION_URL = "https://olympics-62c6a-default-rtdb.firebaseio.com/DataNode/root/DataNode_0/test___csv"
    r = requests.get(p +"/.json")
    data = r.json()

    df = pd.json_normalize(data)

    return df
