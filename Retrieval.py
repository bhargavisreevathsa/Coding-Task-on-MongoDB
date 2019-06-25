# script to retrieve data from database
from pymongo import MongoClient

def retrieve() :

    """ The function retrieve lists the first 50 records of the MongoDB nse.  """
    # Usage of try-catch block to establish connection to MongoDB
    try:
        client = MongoClient('localhost', 27017)
        print("connection established")
    except:
        print("Could not connect")

    # Assigning database and collection
    db = client.nse
    mycol = db.test

    # Fetching data from database and limiting the entries to 50
    for record in mycol.find().limit(50):
        print(record)

    return None

print("The below line is a docstring")
print(retrieve.__doc__)

# Calling the retrieve function
retrieve()

