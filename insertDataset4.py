# Code to insert dataset of 16-06-2019
from pymongo import MongoClient
import pandas
import json

# Reading csv files
csv_data = pandas.read_csv("sec_bhavdata_full_16th_June.csv")
print(csv_data)

# Converting the csv file to JSON data
json_data = json.loads(csv_data.to_json(orient='records'))
print(json_data)

# Populating the database with the json data
try:
    client = MongoClient('localhost', 27017)
    print("connection established")
except:
    print("Could not connect")
db = client.nse
mycol = db.test

mycol.insert(json_data)



