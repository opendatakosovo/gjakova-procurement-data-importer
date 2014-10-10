import csv

from pymongo import MongoClient
from bson import ObjectId

# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.gjakovadata
collection = db.report2011

collection.remove({})
def result():

	with open('report2011.csv', 'rb') as csvfile:
		reader = csv.reader(csvfile, delimiter=',')
		for row in reader:

			
			budget_type = row[0]
			nr = row[1]
			type_of_procurement  = int(row[2])
			value_of_procurement = int(row[3])
			procurement_procedure = int(row[4])
			clasification = int(row [5])
			activity_title_of_procurement = row[6]
			signed_date = row[7]
			contract_value = convert_to_float(row[8])
			contact_price = convert_to_float(row[9])
			aneks_contract_price = row[10]
			company = row[11]
			company_address = row[12]
			due_contract_accepting_time = int(row[13])
			contract_criteria = int(row[14])
			
			report = {
				"budgetType":budget_type,
				"procucementNumber":{
					"nr":nr,
					"typeOfProcurement":type_of_procurement,
					"value_of_procurement":value_of_procurement,
					"procurementProcedure":procurement_procedure
				},
				"clasification":clasification,
				"activityTitleOfProcurement":activity_title_of_procurement,
				"signedDate":signed_date,
				"contractValue":contract_value,
				"contactPrice":contact_price,
				"aneks_contract_price":aneks_contract_price,
				"company":company,
				"company_address":company_address,
				"due_contract_accepting_time":due_contract_accepting_time,
				"contract_criteria":contract_criteria
				
				
				
			}
			collection.insert(report)
def convert_to_float(number):
	return float(number.replace(',',''))
result()



