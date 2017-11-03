municipality-procurement-data-importer
=================================
Importer for procurement data provided by the following  municipalities:
1. Prishtina
2. Gjakova
3. Gjilan
4. Ferizaj
5. Vitia
6. Hani i Elezit

# Installing and running for the first time
1. Install the app: `bash install.sh`
2. Run the importer: `bash run.sh`

## Update database fields

In the newly added data for years 2014, 2015 and 2016 there are some docs 
that contain empty values for company name and residence, also some empty docs. 
Run the script to update these documents with empty values for company name and 
residence with n/a value, and to delete empty docs.

1. Run the script: `bash update_database.sh`