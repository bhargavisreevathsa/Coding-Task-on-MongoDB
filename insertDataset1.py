# Code to insert dataset of 11-06-2019
from pymongo import MongoClient  # Importing MongoClient class from pymongo package
import pandas  # Pandas package used to read csv file
import json  # json package is required to convert csv data to json format

# Reading csv files
csv_data = pandas.read_csv("sec_bhavdata_full_11th_June.csv")
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

