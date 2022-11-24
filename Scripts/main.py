import connection
import sys
# db = client.test

password = sys.argv[1]
if not connection.dbConnection(password):
    print("Failed to connect")
else:
    cluster = connection.dbConnection(password)
    db = cluster.Olympics
    collection = db["test"]

    DataNode1 = db['DataNode1']
    DataNode2 = db['DataNode2']

    document1 = {"_id":4, "college" : "USC"}
    document2 = {"_id":5, "college" : "USC"}

    # collection.insert_many([document1,document2])
    DataNode2.insert_many([document1,document2])
    DataNode1.insert_many([document1,document2])

    results = DataNode1.find()
    for result in results:
        print(result)
