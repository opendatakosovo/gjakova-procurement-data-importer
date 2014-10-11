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
                    type_of_procurement = get_procurement_type(int(row[2]))
                    value_of_procurement = get_procurement_value(int(row[3]))
                    procurement_procedure = get_procurement_procedure(int(row[4]))
                    classification = int(row[5])
                    activity_title_of_procurement = row[6]
                    signed_date = row[7] #TODO: Convert this to Date
                    contract_value = row[8]
                    contract_price = row[9]
                    aneks_contract_price = row[10]
                    company = row[11]
                    company_address = row[12]
                    tipi_operatorit = get_company_type(row[13])
                    afati_kohor = get_due_time(row[14])
                    kriteret_per_dhenje_te_kontrates = get_criteria_type(row[15])

                    report = {
                        "viti": year,
                        "tipiBugjetit": budget_type,
                        "numri": nr,
                        "tipi": type_of_procurement,
                        "vlera": value_of_procurement,
                        "procedura": procurement_procedure,
                        "klasifikimi": classification,
                        "aktiviteti": activity_title_of_procurement,
                        "dataNenshkrimit": signed_date,
                        "kontrata": {
                            "vlera": contract_value,
                            "qmimi": contract_price,
                            "qmimiAneks": aneks_contract_price,
                            "afatiKohor": afati_kohor,
                            "kriteret": kriteret_per_dhenje_te_kontrates
                        },
                        "kompania": {
                            "emri": company,
                            "slug": "",
                            "selia": company_address,
                            "tipiKompanise": tipi_operatorit
                        }
                    }

                    print report
                    print ''

                    collection.insert(report)


def get_buget_type(number):
    if number != "":
        num = int(number)
        if num == 1:
            return "Te hyrat vetanake"
        elif num == 2:
            return "Buxheti i Konsoliduar i Kosoves"
        elif num == 3:
            return "Donacion"
        else:
            return ""
    else:
        return ""


def get_procurement_type(number):
    if number == 1:
        return "Furnizim"
    elif number == 2:
        return "Sherbime"
    elif number == 3:
        return "Sherbime Keshillimi"
    elif number == 4:
        return "Konkurs projektimi"
    elif number == 5:
        return "Pune"
    elif number == 6:
        return "Pune me koncesion"
    elif number == 7:
        return "Prone e palujtshme"
    else:
        return ""


def get_procurement_value(number):
    if number == 1:
        return "Vlere e madhe"
    elif number == 2:
        return "Vlere e mesme"
    elif number == 3:
        return "Vlere e vogel"
    elif number == 4:
        return "Vlere  minimale"
    else:
        return ""


def get_procurement_procedure(number):
    if number == 1:
        return "Procedura e hapur"
    elif number == 2:
        return "Procedura e kufizuar"
    elif number == 3:
        return "Konkurs projektimi"
    elif number == 4:
        return "Procedura e negociuar pas publikimit te njoftimit te kontrates"
    elif number == 5:
        return "Procedura e negociuar pa publikimit te njoftimit te kontrates"
    elif number == 6:
        return "Procedura e kuotimit te Cmimeve"
    elif number == 7:
        return "Procedura e vleres minimale"
    else:
        return ""


def get_company_type(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "OE Vendor"
        elif number == 2:
            return "OE Jo vendor"
    else:
        return ""


def get_due_time(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Afati kohor normal"
        elif number == 2:
            return "Afati kohor i shkurtuar"
    else:
        return ""


def get_criteria_type(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Cmimi me i ulet"
        elif number == 2:
            return "Tenderi ekonomikisht me i favorshem"
    else:
        return ""

parse()
