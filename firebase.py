from cmath import e
from time import sleep
import requests
import json
import sys
import hash

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

def createNameNode(filename, path, numPartition):
    
    filePath = path + "/" + filename

    mkdir(filePath)

    #upload file metadata in the namenode
    d = { "k" : numPartition}
    for i in range(numPartition):
        d["partition_" + str(i)] = DATANODE + "DataNode_" + str(i)
    print(d)
    d = json.dumps(d)
    print(NAMENODE + filePath + ".json")
    r = requests.patch(NAMENODE + filePath + ".json", data = d)

    #Add fileName to each of the DataNodes
    addFileName(filename, numPartition)
    print(r.json())

def createDataNode(fileName, numPartition):

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
        d = { "DataNode_" + str(numberOfDataNodes + 1) : ""}
        d = json.dumps(d)
        numberOfDataNodes += 1

        r = requests.patch(DATANODE + ".json", d)
        print(r.json())

def addFileName(fileName, numPartition):

    r = requests.get(DATANODE + ".json")
    print(r.json())
    count = 0
    for datanode, value in dict(r.json()).items():
        if(count >= numPartition):
            break
        d = {fileName : ""}
        d = json.dumps(d)
        r = requests.patch(DATANODE + datanode + ".json", d)
        print(r.json())
        count += 1

def put(path, filename, numPartition, partitionCol = None):

    #TO-DO READ FILE CONTENTS

    createDataNode(filename, numPartition)
    createNameNode(filename, path, numPartition)

# put("/user/john", "moped___csv", 3)
# addFileName("cars___csv", 5)
while(1):

    command = input(">")
    if(command.split(" ")[0] == "exit"):
        sys.exit("Exiting ....")
    try:
        action, path = command.split(" ")[0], command.split(" ")[1]
        if(action == "mkdir"):
            mkdir(path)
        elif(action == "ls"):
            ls(path)
        elif(action == "rm"):
            rm(path)
        elif(action == "put"):
            filename = command.split(" ")[1]
            #Firebase does not allow the "." character
            filename = filename[:-4] + "___csv"
            path = command.split(" ")[2]
            numPartitions = int(command.split(" ")[3])
            partitionCol = command.split(" ")[4] if len(command.split(" ")) > 4 else None
            put(path, filename, numPartitions, partitionCol)
        else:
            break
    except:
        print("Invalid format")