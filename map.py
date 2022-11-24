import requests
import requests
import json
import sys
import hash
import pandas as pd
import logging

FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

FIREBASE_URL = "https://olympics-62c6a-default-rtdb.firebaseio.com/"
NAMENODE = "https://olympics-62c6a-default-rtdb.firebaseio.com/NameNode/root/"
DATANODE = "https://olympics-62c6a-default-rtdb.firebaseio.com/DataNode/root/"
CURRENT_DIR = "root"

def mapPartition(p, searchColumn, searchQuery):
    # PARTITION_URL = "https://olympics-62c6a-default-rtdb.firebaseio.com/DataNode/root/DataNode_0/test___csv"
    print(p)
    r = requests.get(p +"/.json")
    data = r.json()

    df = pd.json_normalize(data)
    df = df[df[searchColumn] == searchQuery]
    return df
