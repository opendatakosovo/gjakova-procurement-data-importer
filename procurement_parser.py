import csv
import os
from datetime import datetime, date
from slugify import slugify
from pymongo import MongoClient

# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.kosovoprocurements
collection = db.procurements

collection.remove({})

coordinates = {
    "prishtine": {
        "lat": 42.6662068,
        "lon": 21.1599254
    },
    "gjakove": {
        "lat": 42.3911916,
        "lon": 20.4279613
    },
    "bec-gjakove": {
        "lat": 42.431298,
        "lon": 20.2944091
    },
    "rahovec": {
        "lat": 42.4023345,
        "lon": 20.6381607
    },
    "meje-gjakove": {
        "lat": 42.377663,
        "lon": 20.4033923
    },
    "sllup-deqan": {
        "lat": 42.51548,
        "lon": 20.2603555
    },
    "bardhaniq-gjakove": {
        "lat": 42.3911916, # Use gjakove coordinates for this.
        "lon": 20.4279613 # Use gjakove coordinates for this.
    },
    "planqor-gjakove": {
        "lat": 42.4468153,
        "lon": 20.3647685
    },
    "marmull-gjakove": {
        "lat": 42.3763196,
        "lon": 20.5108845
    },
    "skivjan-gjakove": {
        "lat": 42.4324481,
        "lon": 20.37988
    },
    "duzhnje-gjakove": {
        "lat": 42.3695309,
        "lon": 20.351851
    },
    "dejne-rahovec": {
        "lat": 42.4297752,
        "lon": 20.5437363
    },
    "ujez-gjakove": {
        "lat": 42.33815373,
        "lon": 20.53537105
    },
    "novoselle-e-poshtme": {
        "lat": 42.4457221,
        "lon": 20.4066326
    },
    "lipovec-gjakove": {
        "lat": 42.3150608,
        "lon": 20.47715559
    },
    "suhareke": {
        "lat": 42.3555214,
        "lon": 20.8311939
    },
    "gurakoc-istog": {
        "lat": 42.7146085,
        "lon": 20.4252571
    },
    "sheremet-gjakove": {
        "lat": 42.4329554,
        "lon": 20.3287465
    },
    "dardani-gjakove": {
        "lat": 42.3911916,  # Use gjakove coordinates for this.
        "lon": 20.4279613  # Use gjakove coordinates for this.
    },
    "peje": {
        "lat": 42.6606262,
        "lon": 20.2982288
    },
    "junik": {
        "lat": 42.4750556,
        "lon": 20.2757192
    },
    "ferizaj": {
        "lat": 42.3719071,
        "lon": 21.1511922
    },
    "baballoq-deqan": {
        "lat": 42.4796642,
        "lon": 20.3026054
    },
    "prizren": {
        "lat": 42.2181194,
        "lon": 20.7407284
    },
    "shiroke-suhareke": {
        "lat": 42.3443668,
        "lon": 20.8190273
    },
    "koretin-kamenice": {
        "lat": 42.548572,
        "lon": 21.5894437
    },
    "deqan": {
        "lat": 42.5364468,
        "lon": 20.2945804
    },
    "gjinoc-suhareke": {
        "lat": 42.3165359,
        "lon": 20.8158943
    },
    "viti": {
        "lat": 42.3191762,
        "lon": 21.3594618
    },
    "shupkovc-mitrovice": {
        "lat": 42.8799218,
        "lon": 20.888384
    },
    "qagllavice-prishtine": {
        "lat": 42.6127529,
        "lon": 21.1446475
    },
    "podujeve": {
        "lat": 42.911417,
        "lon": 21.193330
    },
    "gjilan": {
        "lat": 42.460879,
        "lon": 21.466578
    },
    "kacanik": {
        "lat": 42.228059,
        "lon": 21.256383
    },
    "lipjan": {
        "lat": 42.524467,
        "lon": 21.126375
    },
    "kllokot": {
        "lat": 42.369029,
        "lon": 21.370978
    },
    "shtime": {
        "lat": 42.438549,
        "lon": 21.037209
    },
    "kamenice": {
        "lat": 42.587380,
        "lon": 21.573784
    },
    "vushtrri": {
        "lat": 42.826968,
        "lon": 20.970568
    },
    "pozheran-viti": {
        "lat": 42.364772,
        "lon": 21.337110
    },
    "mitrovice": {
        "lat": 42.891215,
        "lon": 20.865629
    },
    "gernqar": {
        "lat": 42.327132,
        "lon": 21.415428
    },
    "drenas": {
        "lat": 42.629178,
        "lon": 20.886161
    },
    "fushe-kosove": {
        "lat": 42.637566,
        "lon": 21.093615
    },
    "hani-i-elezit": {
        "lat": 42.150476,
        "lon": 21.297002
    },
    "shterpce": {
        "lat": 42.240943,
        "lon": 21.026023
    },
    "tirane": {
        "lat": 41.327824,
        "lon": 19.818767
    },
    "gjermani": {
        "lat": 51.046960,
        "lon": 10.596942
    }
}


def parse():

    print "Importing procurements data."

    for filename in os.listdir('data/procurements'):
        city = filename
        for filename in os.listdir('data/procurements/'+filename):
            if(filename.endswith(".csv")):
                with open('data/procurements/'+city+"/" + filename, 'rb') as csvfile:
                    reader = csv.reader(csvfile, delimiter=',')
                    line_number = 0
                    for row in reader:
                        year = int(filename.replace('.csv', ''))
                        budget_type = convert_buget_type(row[0])
                        nr = convert_nr(row[1])
                        type_of_procurement = convert_procurement_type(row[2])
                        value_of_procurement = convert_procurement_value(row[3])
                        procurement_procedure = convert_procurement_procedure(row[4])
                        classification = int(convert_classification(row[5]))
                        activity_title_of_procurement = remove_quotes(row[6])
                        signed_date = convert_date(row[7], year, city) #TODO: Convert this to Date
                        contract_value = convert_price(row[8])
                        contract_price = convert_price(row[9])
                        aneks_contract_price = convert_price(row[10])
                        company = remove_quotes(row[11])
                        company_address = remove_quotes(row[12])
                        company_address_slug = slugify(company_address)

                        if company_address_slug == "planqor":
                            company_address_slug = "planqor-gjakove"

                        if company_address_slug == "pozheran":
                            company_address_slug = "pozheran-viti"

                        if company_address_slug == "pozhoran":
                            company_address_slug = "pozheran-viti"

                        if company_address_slug == "gllogoc":
                            company_address_slug = "drenas"

                        if company_address_slug == "fushekosove-prishtine":
                            company_address_slug = "fushe-kosove"

                        if company_address_slug == "podueve":
                            company_address_slug = "podujeve"


                        if company_address_slug == "f-kosove":
                            company_address_slug = "fushe-kosove"

                        if company_address_slug == "prishtie":
                            company_address_slug = "prishtine"

                        if company_address_slug == "gjilan-viti-viti-viti-viti":
                            company_address_slug = "viti"

                        if company_address_slug == "suharek":
                            company_address_slug = "suhareke"

                        if company_address_slug == "kaqanik":
                            company_address_slug = "kacanik"

                        if company_address_slug == "kllokot-kamenice":
                            company_address_slug = "kamenice"


                        tipi_operatorit = convert_company_type(row[13])
                        afati_kohor = convert_due_time(row[14])
                        kriteret_per_dhenje_te_kontrates = convert_criteria_type(row[15])

                        report = {
                            "city": city,
                            "viti": int(year),
                            "tipiBugjetit": budget_type,
                            "numri": nr,
                            "tipi": type_of_procurement,
                            "vlera": value_of_procurement,
                            "procedura": procurement_procedure,
                            "klasifikimiFPP": classification,
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
                                "slug": slugify(company),
                                "selia": {
                                    "emri": company_address,
                                    "slug": company_address_slug
                                },
                                "tipi": tipi_operatorit
                            }
                        }

                        '''
                        if company_address_slug != "":
                            report["kompania"]["selia"]["kordinatat"] = {
                                'gjeresi': coordinates[company_address_slug]['lat'],
                                'gjatesi': coordinates[company_address_slug]['lon']
                            }
                        '''
                        line_number = line_number + 1
                        print str(city) + ": " + "Po procesohet rreshti " + str(line_number) + " viti " + str(year)
                        print report
                        print ''
                        collection.insert(report)


def convert_nr(number):
    if(number is None):
        return ""
    else:

        newNumber = [int(s) for s in number.split() if s.isdigit()]
        if len(newNumber) > 0:
            return int(newNumber[0])
        else:
            return ""


def convert_classification(number):
    if number != "":
        return number
    else:
        return 0

def convert_date(date_str, year, qyteti):
    if date_str.startswith('nd') or date_str.startswith('a') or date_str == "":
        today = date.today()
        today = today.strftime(str("1.1."+str(year)))
        return datetime.strptime(today, '%d.%m.%Y')
    elif date_str.find(",") != -1:
        date_str = date_str.replace(',', '.')
        date_str = date_str[0: 10]
        return datetime.strptime(date_str, '%d.%m.%Y')
    elif date_str.find('/') != -1:
        date_str = date_str.replace('/', '.')
        date_str = date_str[0: 10]
        return datetime.strptime(date_str, '%d.%m.%Y')
    else:
        date_str = date_str[0: 10]

        print str(year) + qyteti
        if len(date_str[6:]) ==2:
            day = date_str[0:2]
            month =  date_str[3:5]
            datet = ""
        
            datet = date_str[6:]
            datet = str(20)+datet            
            final_date = str(day) +"."+str(month)+"." + datet
            return datetime.strptime(final_date, '%d.%m.%Y')
        return datetime.strptime(date_str, '%d.%m.%Y')


def convert_price(num):
    if isinstance(num, str):
        if num.startswith('A') or num.startswith('a') or 'p' in num or num == "":
            return 0
        elif ',' in num:
            return float(num.replace(',', ''))
        else:
            num = num.decode('unicode_escape').encode('ascii', 'ignore')
            return float(num)
    else:
        return float(num)


def remove_quotes(name):
    '''
    if name[0] == '"':
        name = name[1:]

    if name[len(name)-1] == '"':
        name = name[0: (len(name)-1)]
    '''
    return name.replace('"', '')


def convert_buget_type(number):
    if number.find(',') != -1:
        budget_array = []
        if number[:1] == '1':
            budget_array.append("Te hyrat vetanake")
        if number[2:3] == '2':
            budget_array.append("Buxheti i Kosoves")
            return budget_array
        if number[4:5] == '3':
            budget_array.append("Donacion")
            return budget_array

    value = number[:1]
    if value != "":
        num = int(value)
        if num == 1:
            return "Te hyrat vetanake"
        elif num == 2:
            return "Buxheti i Kosoves"
        elif num == 3:
            return "Donacion"
    else:
        return "n/a"


def convert_procurement_type(num):
    if num != "":
        number = int(num)
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
        return "n/a"


def convert_procurement_value(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Vlere e madhe"
        elif number == 2:
            return "Vlere e mesme"
        elif number == 3:
            return "Vlere e vogel"
        elif number == 4:
            return "Vlere  minimale"
    else:
        return "n/a"


def convert_procurement_procedure(num):
    if num != "":
        number = int(num)
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
        return "n/a"


def convert_company_type(num):
    if num != "":
        if num == "Ferizaj":
            return "OE Vendor"
        elif num != "Ferizaj":
            number = int(num)
            if number == 1:
                return "OE Vendor"
            elif number == 2:
                return "OE Jo vendor"
    else:
        return "n/a"


def convert_due_time(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Afati kohor normal"
        elif number == 2:
            return "Afati kohor i shkurtuar"
    else:
        return "n/a"


def convert_criteria_type(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Cmimi me i ulet"
        elif number == 2:
            return "Tenderi ekonomikisht me i favorshem"
    else:
        return "n/a"

parse()
