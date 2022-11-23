from cmath import e
from time import sleep
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

def init():
    data =  requests.get(FIREBASE_URL + ".json")
    print(data.json())

    d = json.dumps({"another_user2" : "user data"})

    r = requests.patch(NAMENODE , data = d)
    print(r.json())

def show_DB(path = None):
    data =  requests.get(FIREBASE_URL + path + ".json")
    print(data.json())

def mkdir(directory):
#input :- "/user/john/keanu"

    levels = directory.split("/")
    PWD = NAMENODE

    for level in levels[1:]:
        d = { level : ""}
        d = json.dumps(d)
        
        g = requests.get(PWD + level + "/.json" )
        if(g.json()):
            PWD = PWD + level + "/"
            continue
        r = requests.patch(PWD + ".json", data=d)
        PWD = PWD + level + "/"

    # show_DB()

def cd(directory):
    
    pass

def pwd(directory):
    pass

def rm(directory):

    levels = directory.split("/")
    PWD = NAMENODE
    for level in levels[1:-1]:
        PWD = PWD + level + "/"
    delete_object = requests.delete(PWD + "/.json" )
    print(delete_object.json())


def ls(directory):

    levels = directory.split("/")
    PWD = NAMENODE
    for level in levels[1:]:
        PWD = PWD + level + "/"

    g = requests.get(PWD + "/.json" )
    if(g.json()):
        levels_dict = g.json()
        for level, _ in levels_dict.items():
            print(level)
    else:
        print("")
        
def asciiConvert(attr):
    attr = str(attr)
    asciiSum = 0
    for c in attr: 
        asciiSum += (ord(c))
    return asciiSum

def hashVal(attr, k):
    asciiSum = asciiConvert(attr)
    return asciiSum%k

def partition(data, k, attr = None):
    d = {}
    for row in data.index:
        attrVal = data.loc[row][attr]
        h = hashVal(attrVal, k)
        if h not in d.keys():
            d[h] = []
        record = data.loc[row].to_json()
        parsed = json.loads(record)
        # d[h].append(data.loc[row].to_json())
        # print(parsed, type(parsed))
        d[h].append(parsed)
    return d

def createNameNode(filename, path, numPartition, partitionColumn):
    
    logging.info("Creating the NameNode template")
    filePath = path + "/" + filename

    mkdir(filePath)

    #upload file metadata in the namenode
    d = { "k" : numPartition}
    d["partitionColumn"] = partitionColumn
    for i in range(numPartition):
        d["partition_" + str(i)] = DATANODE + "DataNode_" + str(i)
    d = json.dumps(d)
    r = requests.patch(NAMENODE + filePath + ".json", data = d)
    print(r.json())
    #Add fileName to each of the DataNodes
    addFileName(filename, numPartition)

def createDataNode(fileName, numPartition):

    logging.info("Creating the DataNode template")
    r = requests.get(DATANODE + ".json")
    if(not r.json()):
        dataNodeTemplate(numPartition, fileName)
    else:
        numberOfDataNodes = len(r.json())
        # print(numberOfDataNodes)
        if(numberOfDataNodes):
            if(numberOfDataNodes > numPartition):
                return
            else:
                dataNodeTemplate(numPartition - numberOfDataNodes, fileName)

def dataNodeTemplate(count, fileName):
    """
    creates the DataNode template;
    if sufficient datanodes exist, nothing happens;
    If Data nodes are not sufficient, new ones are created
    """
    r = requests.get(DATANODE + ".json")
    numberOfDataNodes = len(r.json())

    for i in range(count):
        d = { "DataNode_" + str(numberOfDataNodes) : ""}
        d = json.dumps(d)
        numberOfDataNodes += 1

        r = requests.patch(DATANODE + ".json", d)

def addFileName(fileName, numPartition):

    r = requests.get(DATANODE + ".json")
    # print(r.json())
    count = 0
    for datanode, value in dict(r.json()).items():
        if(count >= numPartition):
            break
        # print("**** ADD FILE NAME VALUE", value)
        if(fileName in value):
            continue
        d = {fileName : ""}
        d = json.dumps(d)
        r = requests.patch(DATANODE + datanode + ".json", d)
        count += 1

def put(path, filename, numPartition, partitionCol = None):
    
    #To send to partition function

    data = pd.read_csv(filename[:-6] + ".csv")
    data = data.iloc[8:12]

    if not partitionCol:
        partitionCol = data.keys()[0]
    bucketDict = partition(data, numPartition, partitionCol)

    #TO-DO READ FILE CONTENTS

    createDataNode(filename, numPartition)
    createNameNode(filename, path, numPartition, partitionCol)

    pushDataToDataNode(bucketDict, filename, path)

def getPartitionLocations(filename, path):
    """
    return : Locations of all partitions
    """
    logging.info("Looking for all partition in the NameNode")
    r = requests.get(NAMENODE + path[1:] + "/" + filename +".json?print=pretty")
    locations = r.json()
    if not locations:
        print("No content exists")
    else:
        del locations['k']
        del locations['partitionColumn']
        return locations

def pushDataToDataNode(data, filename, path):
    """
    Pushes the hashed rows into the pertinent DataNode
    """

    logging.info("Pushing Data to the respective buckets in DataNode")
    locations = getPartitionLocations(filename, path)

    for key, value in locations.items():
        if(int(key[-1]) in data):
            datanode_URI = value + "/" + filename + "/.json"
            print(datanode_URI)
            # print(json.dumps(data[int(key[-1])][:2]))

            r = requests.get(datanode_URI)
            bucketData = []
            if(r.json()):
                bucketData = r.json()
            bucketData.extend(data[int(key[-1])])

            d = {filename : bucketData}
            d = json.dumps(d)
            r = requests.patch(value + ".json", d)
            # print(r.json())
  
def printPartitionLocations(locations):
    """
    prints the location JSON object in the format --> Partition number : Location in firebase
    """
    for key, value in locations.items():
            print(key, " : ",value)


# getPartitionLocations("test___csv", "/user/smaran")
put("/user/hi", "test___csv", 3)
# addFileName("cars___csv", 5)
# while(1):

#     command = input(">")
#     if(command.split(" ")[0] == "exit"):
#         sys.exit("Exiting ....")
#     try:
#         action, path = command.split(" ")[0], command.split(" ")[1]
#         if(action == "mkdir"):
#             mkdir(path)
#         elif(action == "ls"):
#             ls(path)
#         elif(action == "rm"):
#             rm(path)
#         elif(action == "put"):
#             filename = command.split(" ")[1]
#             #Firebase does not allow the "." character
#             filename = filename[:-4] + "___csv"
#             path = command.split(" ")[2]
#             numPartitions = int(command.split(" ")[3])
#             partitionCol = command.split(" ")[4] if len(command.split(" ")) > 4 else None
#             put(path, filename, numPartitions, partitionCol)
#         elif(action == "getPartitionLocations"):
#             filename = command.split(" ")[1]
#             #Firebase does not allow the "." character
#             filename = filename[:-4] + "___csv"
#             locations = getPartitionLocations(filename, path)
#             printPartitionLocations(locations)
#         else:
#             break
#     except FileNotFoundError:
#         print("File not found")
#     except Exception as e:
#         logging.error("Failure" + repr(e))