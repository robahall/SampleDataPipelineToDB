""" Main run file for analyzing instrumental data. Use this file to analyze and connect to database.
Note: Password only asks properly in cmd line and not PyCharm or IDE.

"""

import getpass
import logging
import os
from data_extract import DataExtraction


# Store Info
id = input("Enter Username: ")
password = getpass.getpass()

# Log any errors
logging.basicConfig(filename='Instrument Transmission Errors.log', level=logging.DEBUG, format='%(asctime)s %(message)s')


# Create list of files in directory
datadir = []

for filenames in os.listdir(os.getcwd() + "\\ToBeParsedData"):
	datadir.append(filenames)

print(datadir)

for f in datadir:
	try:
		# Create object file
		file = DataExtraction(filename = f)

		# Split filename
		file.split_data()

		# Extract Data
		file.extract_data()

		# Use MySQL Database to store TGA Data
		#file.DatabaseConnectionMySQL(id, password)

		# Use MSSQL Database to store TGA Data
		file.DatabaseConnectionMSSQL(id, password)

	except:
		logging.exception("Exception:")
		continue
