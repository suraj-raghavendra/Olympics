from pymongo import MongoClient


cluster = MongoClient("mongodb+srv://dsci551_group71:Group71DSCI551@cluster0.cembryp.mongodb.net/?retryWrites=true&w=majority")
# db = client.test

db = cluster.Olympics
collection = db["test"]

document1 = {"_id":2, "college" : "USC"}
document2 = {"_id":3, "college" : "USC"}

collection.insert_many([document1,document2])

results = collection.find()
for result in results:
    print(result)
