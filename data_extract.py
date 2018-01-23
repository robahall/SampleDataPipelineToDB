""" Creates a class for extraction of data from raw file from instrument.

Includes: filename, ID, substrate type, calculated weight percent and slope.

Also allows movement of file and interaction with MySQL or SQL Server database.


"""




## Standard packages
import os
import re
import csv
import datetime
import shutil
import itertools


# Database tools
from sqlalchemy import *

def previous_data(theiterable):
	"""Allows a function to pull the previous item in an iteration
	"""
	prevs, items = itertools.tee(theiterable, 2)
	prevs = itertools.chain([None], prevs)
	return itertools.zip_longest(prevs, items)

def move_file_to_folder(filename, source, destination):
	"""Move files from folder to folder"""
	shutil.move(source + filename, destination + filename)
	complete = "File %s moved from %s to %s" % (filename, source, destination)
	#print(complete)


class DataExtraction:
	""" Creates a dictionary of filename, ID, Substrate, weight percent and slope """



	def __init__(self, filename = None, file_dic = None):
		self.filename = filename
		self.file_dic = file_dic


	def print_filename(self):
		print(self.filename)


	def print_file_dic(self):
		print(self.file_dic)


	def split_data(self):
		""" Splits the filename into applicable information including run time, substrate type"""

		Datesplit = re.findall('([0-9]\S*_[0-9]*)_', self.filename)

		Datesplit = datetime.datetime(int('20' + Datesplit[0][4:6]), int(Datesplit[0][0:2]),
		                               int(Datesplit[0][2:4]), int(Datesplit[0][7:9]),
		                               int(Datesplit[0][9:11]), int(Datesplit[0][11:13])
		                               )

		Runsplit = re.findall('[0-9]*_[0-9]*_([a-zA-Z0-9]*)_', self.filename)

		Subsplit = re.findall('[0-9]*_[0-9]*_[a-zA-Z0-9]*_([a-zA-Z0-9]*).', self.filename)


		#Original names in order Date, RunName, Substrate, filename
		self.file_dic = dict([("Date", Datesplit), ("RunName", Runsplit[0]),
		                      ("SubstrateName", Subsplit[0]), ("Filename", self.filename)])

		return self.file_dic


	def extract_data(self):
		"""Parses a data file from instrument and extracts the weight percent and slope.
			"""
		SOURCE = os.getcwd() + '\\ToBeParsedData\\'  # Change string \\ToBeParsedData\\ to change path of source folder

		datafile = self.file_dic
		filename = datafile.get("Filename")

		values = []
		count = 0



		with open(SOURCE + filename, newline='') as f:
			r = csv.reader(f, delimiter='\t')
			r.__next__()
			data = [row for row in r]

		start = float(data[0][2])

		for previous, item in previous_data(data):
			### Find error code from gas switch


			#Find the location of the error point and store the timestamp
			if item[0] == '-3.000000':
				count += 1
				values.append(previous[0])
			elif count == 2:
				break
			else:
				continue

		# Initial location of error code thrown. It provides both the first and second time. We use only the first now.

		initial = float(values[0])

		substrate_type = self.file_dic['SubstrateName']


		# Where to take the measurement. The value is the time in minutes.
		# Others are all other substrates

		special_substrate = 20.0
		others = 5.0

		# Performs a regular expression search for the special substrate type.

		is_it_BFB = re.match('(BFB)', substrate_type)
		is_it_DS = re.match('(DS)', substrate_type)

		if is_it_BFB or is_it_DS:
			#print("Is special Class!.")
			at_initial = 0
			at_final = 0
			count_initial = 0
			count_final = 0


			# Takes the average measure over about 5 data points, 0.1 minutes before up till the error code.

			for row in data:
				if float(row[0]) > initial- 0.1 and float(row[0]) <= initial:
					at_initial += float(row[2])
					count_initial += 1

				elif float(row[0]) > initial + special_substrate - 0.1 and float(row[0]) <= initial + special_substrate:
					at_final += float(row[2])
					count_final += 1

				else:
					continue

			avg_initial = at_initial/count_initial
			avg_final = at_final/count_final
			percent_change = (float(avg_final) - float(avg_initial))*100.0 / start

		else:
			#print("Is not special class!")
			at_initial = 0
			at_final = 0
			count_initial = 0
			count_final = 0

			# Takes the average measure over about 5 data points, 0.1 minutes before up till the error code.
			for row in data:
				if float(row[0]) > initial - 0.1 and float(row[0]) <= initial:
					at_initial += float(row[2])
					count_initial += 1
				elif float(row[0]) > initial + others - 0.1 and float(row[0]) <= initial + others:
					at_final += float(row[2])
					count_final += 1
				else:
					continue

			# This value should really be in ppm not weight percent. Would be an easier value to use.
			# Just saying!

			avg_initial = at_initial / count_initial
			avg_final = at_final / count_final
			percent_change = (float(avg_final) - float(avg_initial)) * 100.0 / start


		print(percent_change)


		# Calculate slope. Slope larger than 1.0E-4 should be removed due to faulty instrument reading.
		# This section the slope should be flat.

		at40 = 0
		at40t = 0
		at55 = 0
		at55t = 0
		count40 = 0
		count55 = 0


		for row in data:
			if float(row[0]) > 40.00 and float(row[0]) <= 40.1:
				at40 += float(row[2])
				at40t += float(row[0])
				count40 += 1
			elif float(row[0]) > 55.00 and float(row[0]) <= 55.1:
				at55 += float(row[2])
				at55t += float(row[0])
				count55 += 1
			else:
				continue

		avg55 = at55 / (count55 * start)
		avg40 = at40 / (count40 * start)
		avg55t = at55t / (count55 * start)
		avg40t = at40t / (count40 * start)

		checkslope = (avg55 - avg40) / (avg55t - avg40t)

		self.file_dic['WeightPercent'] = percent_change
		self.file_dic['Slope'] = checkslope

		return self.file_dic


	def DatabaseConnectionMySQL(self, id, password):
		""" This function appends data from a dictionary to our database.
		As long as the dictionary header matches the table the import will succeed
		"""

		SOURCE = os.getcwd() + '\\ToBeParsedData\\'  # Change string \\ToBeParsedData\\ to change path of source folder
		DESTINATION = os.getcwd() + '\\ParsedData\\'  # Change string \\ParsedData\\ to change path of source folder


		try:
			# Connect to database
			db = create_engine(
				"mysql+pymysql://" + id + ":" + password + '@<IP-Address>:<port>/DB')

			# Read SQL statements SQLAlchemy sends. True shows the statements sent. MetaData creates the database object.
			db.echo = False
			metadata = MetaData(db)

			# Create Schema in SQLAlchemy.
			data_to_db = Table('results', metadata, autoload=True)

			i = data_to_db.insert()

			# Extracts data from parse function
			# print(self.file_dic)
			i.execute(self.file_dic)

			# print("Successful transfer!")
			move_file_to_folder(self.filename, SOURCE, DESTINATION)



		except Exception as e:
			print(str(e))


	def DatabaseConnectionMSSQL(self, id, password):
		""" This function appends data from a dictionary to our database.
		As long as the dictionary header matches the table the import will succeed
		"""

		try:
			# Connect to database
			db = create_engine('mssql+pymssql://' + id + ':' + password + '@<IP-Address>:<port>/Db')

			# Read SQL statements SQLAlchemy sends. True shows the statements sent. MetaData creates the database object.
			db.echo = True
			metadata = MetaData(db)

			# Create Schema in SQLAlchemy.
			data_to_msdb = Table('Instrument', metadata, autoload=True)

			# Create a select statement to query database
			stmt = select([data_to_msdb])

			# Query database
			db_results = db.execute(stmt)

			print(db_results.fetchall())

		except Exception as e:
			print(str(e))



if __name__ == '__main__':
	file = DataExtraction('<filename>')
	file.print_filename()
	file.split_data()
	print(file.file_dic)