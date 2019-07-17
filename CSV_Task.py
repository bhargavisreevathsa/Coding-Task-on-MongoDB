# Sample code to handle CSV data in MongoDB
# Importing MongoClient class from pymongo package to connect to database
from pymongo import MongoClient
import glob    # To read multiple files
import csv     # To handle csv files


# Subroutine to Connect to database
def connect():

    """ The function connects to MongoDB stock_exchange.  """
    # Usage of try-catch block to establish connection to MongoDB
    try:
        client = MongoClient('localhost', 27017)
        print("connection established")
    except:
        print("Could not connect")
        exit(0)

    # Assigning database
    db = client.stock_exchange
    return db


# Subroutine to insert
def insert(stock_database):

    """ The function inserts data to MongoDB stock_exchange.  """
    print(stock_database)
    # Reading multiple files
    list_of_files = glob.glob('./*.csv')
    print(list_of_files)
    for file in list_of_files:
        # Reading csv files
        csv.register_dialect('myDialect', delimiter=',',skipinitialspace=True)
        with open(file, 'r') as csvFile:
            reader = csv.reader(csvFile, dialect='myDialect')
            for row in reader:
                if row[0] == 'SYMBOL':
                    continue
                print(row[0], row[1], row[2], row[3], row[4])
                company_tuple = {"symbol": row[0], "series": row[1]}
                # Creating a collection company that stores symbol and series of company
                try:
                    id = stock_database.company.insert_one(company_tuple)
                    company_id = id.inserted_id
                    print(company_id)
                except:
                    print("Cannot create the Collection company")

                data_per_day = {"_id": company_id, "DATE": row[2], "PREV_CLOSE":row[3], "OPEN_PRICE": row[4],
                                   "HIGH_PRICE": row[5], "LOW_PRICE": row[6], "LAST_PRICE": row[7],
                                   "CLOSE_PRICE": row[8], "AVG_PRICE": row[9], "TOT_TRD_QTY": row[10],
                                   "TURN_OVER_LACS": row[11], "NUM_OF_TRADES": row[12],
                                   "DELIVER_QTY": row[13], "DELIVER_PER": row[14]}
                # Creating a collection to hold other data of the day pertaining to company
                try:
                    stock_database.trade_details.insert_one(data_per_day)
                except:
                    print("Cannot create the collection trade_details")
            csvFile.close()
    return None


# Main Module
database = connect()
print(database)
insert(database)





