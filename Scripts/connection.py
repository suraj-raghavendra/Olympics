from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.errors import OperationFailure

def dbConnection(password):
    client = MongoClient("mongodb+srv://dsci551_group71:"+password+"@cluster0.cembryp.mongodb.net/?retryWrites=true&w=majority")
    try:
        client.admin.command('ismaster')
        return client
    except ConnectionFailure as err:
        print(f"Data Base Connection failed. Error: {err}")
        return False

# cluster = MongoClient("mongodb+srv://dsci551_group71:Group71DSCI551@cluster0.cembryp.mongodb.net/?retryWrites=true&w=majority")