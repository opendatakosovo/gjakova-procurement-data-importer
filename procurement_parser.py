import csv
import os

from pymongo import MongoClient

# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.gjakova
collection = db.procurements

collection.remove({})


def parse():

    print "Importing procurements data."

    for filename in os.listdir('data/procurements'):
        if(filename.endswith(".csv")):

            with open('data/procurements/' + filename, 'rb') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                for row in reader:

                    year = int(filename.replace('.csv', ''))
                    budget_type = get_buget_type(row[0])
                    nr = row[1]
                    type_of_procurement = int(row[2])
                    value_of_procurement = int(row[3])
                    procurement_procedure = int(row[4])
                    classification = int(row[5])
                    activity_title_of_procurement = row[6]
                    signed_date = row[7] #TODO: Convert this to Date
                    contract_value = row[8]
                    contract_price = row[9]
                    aneks_contract_price = row[10]
                    company = row[11]
                    company_address = row[12]
                    #due_contract_accepting_time = int(row[13])
                    #contract_criteria = int(row[14])

                    report = {
                        "year": year,
                        "budgetType": budget_type,
                        "number": nr,
                        "type": type_of_procurement,
                        "value": value_of_procurement,
                        "procedure": procurement_procedure,
                        "classification": classification,
                        "activity": activity_title_of_procurement,
                        "signedDate": signed_date,
                        "contract": {
                            "value": contract_value,
                            "price": contract_price,
                            "aneksPrice": aneks_contract_price,
                        },
                        "company": {
                            "name": company,
                            "slug": "",
                            "address": company_address,
                        }
                    }

                    print report
                    print ''

                    collection.insert(report)


def get_buget_type(number):
    if number == 1:
        return "Te hyrat vetanake"
    elif number == 2:
        return "Buxheti i Konsoliduar i Kosoves"
    elif number == 3:
        return "Donacion"
    else:
        ""

parse()
