import csv

from pymongo import MongoClient
from bson import ObjectId

# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.gjakovadata
collection = db.procurement2010

def result():

	with open('Gjakova_2011.csv', 'rb') as csvfile:
		reader = csv.reader(csvfile, delimiter=',')
		for row in reader:
			group = row[0]
			forecast = row[1]
			commune = row[2]
			type1 = row[3]
			type2 = row[4]
			type3 = row[5]
			total = row[8]
			date = row[9]
		
			#print grupi + parashikimi + komuna + tipi1 + tipi2 + tipi3 + shuma+ data
			
			doc = {
				"group":group,
				"forecast":forecast,
				"commune":commune,
				"type1":type1,
				"type2":type2,
				"type3":type3,
				"total":total,
				"date":date
				
			}
			print doc
			collection.insert(doc)

result()



