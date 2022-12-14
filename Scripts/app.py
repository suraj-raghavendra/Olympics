#app.py>

from cmath import e
from os import path
from time import sleep
from numpy import maximum, minimum
import requests
import json
import sys
import pandas as pd
import logging
import map
import analytics
from pandas.api.types import is_object_dtype, is_numeric_dtype, is_bool_dtype

from map import FIREBASE_URL

FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

# FIREBASE_URL = "https://olympics-62c6a-default-rtdb.firebaseio.com/"
FIREBASE_URL = "https://dsci-551-17cee-default-rtdb.firebaseio.com/"
NAMENODE = FIREBASE_URL + "NameNode/root/"
DATANODE = FIREBASE_URL + "DataNode/root/"
CURRENT_DIR = "root"
FILENAME = FIREBASE_URL + "/FileName"

def init():

    r = requests.get(NAMENODE + ".json")
    if(not r.json()):
        d = { "NameNode" : 
                           {
                               "root" : ""
                           },  
                }
        d = json.dumps(d)
        f = requests.patch(FIREBASE_URL + ".json", data = d)
        logging.info("NameNode created")
    logging.info("NameNode already present")

    r = requests.get(DATANODE + ".json")
    if(not r.json()):
        d = { "DataNode" : 
                           {
                               "root" : ""
                           },  
                }
        d = json.dumps(d)
        f = requests.patch(FIREBASE_URL + ".json", data = d)
        logging.info("DataNode created")
    logging.info("DataNode already present")

    r = requests.get(FILENAME + ".json")
    if(not r.json()):
        d = { "FileName" : ""}
        d = json.dumps(d)
        f = requests.patch(FIREBASE_URL + ".json", data = d)
        logging.info("FileNode created")
    logging.info("FileNode already present")

def show_DB(path = None):
    data =  requests.get(FIREBASE_URL + path + ".json")
    print(data.json())

def mkdir(directory):
#input :- "/user/john/keanu"
    # print(directory)
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
#     print("Succesfully added directory")

    # show_DB()

def cd(directory):
    
    pass

def pwd(directory):
    pass

def rm(filename):
#     print(directory)
    filenameParsed = fileNameParser(filename)
    r = requests.get(FILENAME + ".json")
    filePaths = r.json()
    logging.info("Remove function")
    if(filenameParsed in filePaths):
        filePath = filePaths[filenameParsed]
        # r = requests.get(NAMENODE + filePath + ".json")
        # r = r.json()
        locations = getPartitionLocations(filename, filePath)
        del locations['k']
        del locations['partitionColumn']
        # for partitionNumber, partitionURI in locations.items():
        #     uriSoftDelete(partitionURI, filenameParsed)
        uriSoftDelete(NAMENODE + filePath,filenameParsed)  #remove from NameNode
        # uriSoftDelete(FILENAME , filenameParsed)

    else:
        print("File Not Found")


def uriSoftDelete(uri, filename):
    r = requests.get(uri + ".json")
    node = r.json()
    node[filename] = {}
    # for key, value in 
    print(r.json().keys())
    print(uri)
    print(node.keys())
    node = json.dumps(node)
    r = requests.patch(uri + ".json", data = node)





def ls(directory):

    levels = directory.split("/")
    PWD = NAMENODE
    for level in levels[1:]:
        PWD = PWD + level + "/"

    g = requests.get(PWD + "/.json" )
    if(g.json()):
        levels_dict = g.json()
        for level, _ in levels_dict.items():
            if(level[-6:] == "___csv"):
                level = "File      : " + level[:-6] + ".csv"
                print(level)
                continue
            print("Directory : " + level)
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

    logging.info("Adding File name to the DataNodes")
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

def createFileNodeEntry(filename, path):
    d = { filename : path}
    d = json.dumps(d)
    r = requests.patch(FILENAME + ".json", data = d)
    logging.info("Created FileNode entry")

def put(path, filename, numPartition, partitionCol = None):
#     print(path, filename, numPartition, partitionCol)
    #To send to partition function
#     filename = fileNameParser(filename)
    data = pd.read_csv(filename)
    data = data.iloc[:50] #TO-DO REMOVE
    filenameParsed = fileNameParser(filename)
    print("original: ", filename)
    print("parsed: ", filenameParsed)
#     data = data.iloc[:]
    # print(data)

    if not partitionCol:
        partitionCol = data.keys()[0]
    bucketDict = partition(data, numPartition, partitionCol)
    createFileNodeEntry(filenameParsed, path)
    createDataNode(filenameParsed, numPartition)
    createNameNode(filenameParsed, path, numPartition, partitionCol)
    pushDataToDataNode(bucketDict, filename, path)
    logging.info("File successfully uploaded to EDFS")
    print("File successfully added to EDFS")

def getPartitionLocations(filename, path):
    """
    return : Locations of all partitions
    """
    logging.info("Looking for all partitions in the NameNode")
    filename = fileNameParser(filename)
    r = requests.get(NAMENODE + path[1:] + "/" + filename +".json?print=pretty")
    locations = r.json()
    print("line 238", locations, NAMENODE + path[1:] + "/" + filename +".json?print=pretty")
    if not locations:
        print("No content exists")
        return None
    else:
        return locations

def pushDataToDataNode(data, filename, path):
    """
    Pushes the hashed rows into the pertinent DataNode
    """

    logging.info("Pushing Data to the respective buckets in DataNode")
    locations = getPartitionLocations(filename, path)
    filenameParsed = fileNameParser(filename)
    del locations['partitionColumn']
    del locations['k']
    for key, value in locations.items():
        if(int(key[-1]) in data):
            datanode_URI = value + "/" + filenameParsed + "/.json"
            # print(json.dumps(data[int(key[-1])][:2]))
            # print(datanode_URI)
            r = requests.get(datanode_URI)
            # print("line 298",r.json())
            bucketData = []
            if(r.json()):
                bucketData = r.json()
            bucketData.extend(data[int(key[-1])])

            d = {filenameParsed : bucketData}
            d = json.dumps(d)

            r = requests.patch(value + ".json", d)

  
def printPartitionLocations(locations):
    """
    prints the location JSON object in the format --> Partition number : Location in firebase
    """
    for key, value in locations.items():
            print(key, " : ",value)

def combineDF(df1, df2):
    return pd.concat([df1, df2])


def search(filename, searchColumn, searchQuery):
    filenameOriginal = filename
    filename = fileNameParser(filename)
    r = requests.get(FILENAME + ".json")
    filePaths = r.json()
    if(filename in filePaths):
        filePath = filePaths[filename]
        logging.info("File Found")
        file_URI = NAMENODE + filePath
        locations = getPartitionLocations(filenameOriginal, filePath)
        if not locations:
            logging.info("Enter the correct column name")
            print("Please enter the correct column  name")
            return
        partitionColumn = locations['partitionColumn']
        k = locations['k']
        resDF = pd.DataFrame()
        if(partitionColumn == searchColumn):
            hashValue = hashVal(searchQuery, k)
            partition_URI = locations["partition_" + str(hashValue)] + "/" + filename
            df = map.mapPartition(partition_URI)
            df = df[df[searchColumn] == searchQuery]
            resDF = df
        else:
            logging.info("Fetching data from all partitions")
            for key, value in locations.items():
                if(key == "k" or key == "partitionColumn"):
                    continue
                partition_URI = value + "/" +filename
                df = map.mapPartition(partition_URI)
                df = df[df[searchColumn] == searchQuery]
                resDF = combineDF(resDF, df)
        print(resDF)
    else:
        logging.info("File not found")
        print("File Not found")

def dataAnalytics(filename, column, analyticFunction):

    filenameOriginal = filename
    filename = fileNameParser(filename)
    r = requests.get(FILENAME + ".json")
    filePaths = r.json()
    if(filename in filePaths):
        filePath = filePaths[filename]
        logging.info("File Found")
        file_URI = NAMENODE + filePath
        locations = getPartitionLocations(filenameOriginal, filePath)
        if not locations:
            logging.info("Enter the correct column name")
            print("Please enter the correct column  name")
            return
        logging.info("Fetching data from all partitions")
        resDF = pd.DataFrame()
        for key, value in locations.items():
            if(key == "k" or key == "partitionColumn"):
                continue
            partition_URI = value + "/" +filename
            df = map.mapPartition(partition_URI)
            resDF = combineDF(resDF, df)

        if(not is_numeric_dtype(df[column])):
            print("Column does not contain numeric values")
            return
        if(analyticFunction == "mean"):
            average = analytics.mean(resDF, column)
            print("AVERAGE = ", average)
            return
        if(analyticFunction == "minimum"):
            minimum = analytics.minimum(resDF, column)
            print(f"Minimum = {minimum}")
            return
        if(analyticFunction == "maximum"):
            maximum = analytics.maximum(resDF, column)
            print(f"Maximum = {maximum}")
            return
        if(analyticFunction == "range"):
            range = analytics.maximum(resDF, column) - analytics.minimum(resDF, column)
            print(f"Range = {range}")
            return
        if(analyticFunction == "standardDeviation"):
            stdDev = analytics.standardDeviation(resDF, column)
            print(f"Standard Deviation = {stdDev}")
            return

        if(analyticFunction == "median"):
            median = analytics.median(resDF, column)
            print(f"Median = {median}")
            return
        if(analyticFunction == "mode"):
            mode = analytics.mode(resDF, column)
            print(f"Mode = {mode}")
            return
    else:
        logging.info("File not found")
        print("File Not found")

def readPartitionData(filename, partitionNumber, numLines = "5"):

    logging.info("Read Partition Data")
    numLines = int(numLines)
    r = requests.get(FILENAME + ".json")
    filePaths = r.json()
    filenameOriginal = filename
    filename = fileNameParser(filename)
    if(filename in filePaths):
        filePath = filePaths[filename]
        logging.info("File Found")
        file_URI = NAMENODE + filePath
        locations = getPartitionLocations(filenameOriginal, filePath)
        resDF = pd.DataFrame()
        
        if("partition_" + partitionNumber in locations):
            partition_URI = locations["partition_" + partitionNumber] + "/" +filename
            resDF = map.mapPartition(partition_URI)
            print(resDF.head(numLines))
        else:
            print("Incorrect partition number")
    else:
        print("File Not found")


def cat(filename, numLines = 5):
    """
    Accepts filename and number of lines to print in the file, by default, number of lines = 5 
    """
    # filename = fileNameParser(filename)
    logging.info("Cat function")
    r = requests.get(FILENAME + ".json")
    filePaths = r.json()
    filenameOriginal = filename
    filename = fileNameParser(filename)
    if(filename in filePaths):
        filePath = filePaths[filename]
        logging.info("File Found")
        file_URI = NAMENODE + filePath
        locations = getPartitionLocations(filenameOriginal, filePath)
        partitionColumn = locations['partitionColumn']
        resDF = pd.DataFrame()
        logging.info("Fetching data from all partitions")
        for key, value in locations.items():
            if(key == "k" or key == "partitionColumn"):
                continue
            partition_URI = value + "/" +filename
            df = map.mapPartition(partition_URI)
            resDF = combineDF(resDF, df)
        print(resDF.head(numLines))
    else:
        print("File Not found")

def fileNameParser(filename):
    filename = filename[:-4] + "___csv"
    return filename

# init()
# rm("athlete_events.csv")
# readPartitionData("athlete_events.csv", "1", "8")
# cat("athlete_events.csv",50)
# dataAnalytics("athlete_events.csv", "Age", "range")
# dataAnalytics("test___csv", "Height", "standardDeviation")
# dataAnalytics("test___csv", "Height", "minimum")
# dataAnalytics("test___csv", "Height", "maximum")
# dataAnalytics("test___csv", "Height", "mean")
# dataAnalytics("test___csv", "Height", "median")
# dataAnalytics("test___csv", "Height", "mode")
# search("test___csv", "Name", "John Aalberg")
# init()
# getPartitionLocations("test___csv", "/user/smaran")
# put("/user/hell", "athlete_events.csv", 4)
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