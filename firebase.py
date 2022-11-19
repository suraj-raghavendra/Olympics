from cmath import e
import requests
import json

FIREBASE_URL = "https://olympics-62c6a-default-rtdb.firebaseio.com/"
NAMENODE = "https://olympics-62c6a-default-rtdb.firebaseio.com/NameNode/root/"
CURRENT_DIR = "root"

def init():
    data =  requests.get(FIREBASE_URL + ".json")
    print(data.json())

    d = json.dumps({"another_user2" : "user data"})

    r = requests.patch(NAMENODE , data = d)
    print(r.json())

def show_DB():
    data =  requests.get(FIREBASE_URL + ".json")
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

    show_DB()
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

while(1):

    command = input(">")
    try:
        action, path = command.split(" ")[0], command.split(" ")[1]
        if(action == "mkdir"):
            mkdir(path)
        elif(action == "ls"):
            ls(path)
        elif(action == "rm"):
            rm(path)
        else:
            break
    except:
        print("Invalid format")

    



    

