from cmath import e
from matplotlib.font_manager import json_dump
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
# mkdir("/user/chauhan/mahak")

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
    filename = filename[:-4] + "___csv"
    filePath = path + "/" + filename
    mkdir(filePath)

    #upload file metadata in the namenode
    d = { "k" : numPartition}
    d = json.dumps(d)
    r = requests.patch(NAMENODE + filePath + ".json", data= d)

    createDataNode(filename, numPartition)

def createDataNode(fileName, numPartition):

    r = requests.get(DATANODE + ".json")
    numberOfDataNodes = len(r.json())
    if(numberOfDataNodes):
        if(numberOfDataNodes > numPartition):
            return
        else:
            dataNodeTemplate(numPartition - numberOfDataNodes)
    else:
        dataNodeTemplate(numPartition)



def dataNodeTemplate(count):
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


createNameNode("cars.csv", "/user/suraj", 4)

def put(path, filename, numPartition, partitionCol = None):

    #TO-DO READ FILE CONTENTS
    createNameNode(filename, path, numPartition)

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
#             filename = path
#             path = command.split(" ")[2]
#             numPartitions = command.split(" ")[3]
#             partitionCol = command.split(" ")[4] if command.split(" ")[4] else None
#             put(filename, path, numPartitions, partitionCol)
#         else:
#             break
#     except:
#         print("Invalid format")