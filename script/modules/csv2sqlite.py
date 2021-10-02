import os, csv, sqlite3

# importing custom modules
if __name__ == '__main__':
	import common_functions
else:
	from modules import common_functions

class Csv2sqlite:
	"""turns a list of csv files into tables in a new sqlite database.
	The newly created sqlite database will have a name like 'SEM_NER_200110110426.sqlite'
	returns the full path of the newly created db and the number of records in each.
	"""
	def __init__(self, csvfolderpath, db_output_path, unique_id_fieldname, logger, ignore_testdata):
		
		self.logger = logger
		self.logger.info('\n')		
		self.logger.info('--> Running csv2sqlite module')
		self.csvfolderpath = csvfolderpath # where the csv files are stored
		self.db_path = db_output_path # where you want to save the newly created sqlite file
		self.unique_id_fieldname = unique_id_fieldname
		self.ignore_testdata = ignore_testdata

		# self.overwrite = overwrite  <- inactive. delete this unless you need non-overwriting option.
		self.db_name = ''
		self.db_fullpath_new = ''
		self.tablenames_n_rec_count = {}

		# this fieldname will be used as the new primary key field when moving the csv files to the sqlite.


		self.generate_db_name()
		self.getcsvfilelist()
		self.csv_to_sqlite()

		self.logger.debug('db_fullpath_new = %s'%self.db_fullpath_new)
		self.logger.debug('tablenames_n_rec_count = \n%s'%self.tablenames_n_rec_count)




	def generate_db_name(self):
		
		# name of the new dbf will be RAP_200110105924 where the number is the date and time.
		prefix = "RAP_"
		suffix = ".sqlite"
		datetime = common_functions.datetime_stamp()
		self.db_name = '%s%s%s'%(prefix,datetime,suffix)  # eg. RAP_200110110426.sqlite
		self.db_fullpath_new = os.path.join(self.db_path, self.db_name) # eg. C:\DanielKim\temp\RAP_200110110426.sqlite





	def getcsvfilelist(self):
		"""
		creates a list of csv file paths based on the input csv folder path
		"""
		if os.path.isdir(self.csvfolderpath):
			self.csvfile_list = [os.path.join(self.csvfolderpath,file) for file in os.listdir(self.csvfolderpath) if file.upper()[-4:] == '.CSV']
			if len(self.csvfile_list) == 0:
				self.logger.info('*** ERROR: No csv file found in the directory: %s'%self.csvfolderpath)
		else:
			self.logger.info('*** ERROR: The directory  %s  does not exist'%self.csvfolderpath)




	def csv_to_sqlite(self):
		"""
		This module assumes that the fieldnames in those csv files are unique and have no special character.
		Reads the input csv files and outputs it into the sqlite database.
		This module is not specific to RAP project csv files, and can be applied to any csv files.
		"""

		for csv_fullpath in self.csvfile_list:

			csvfile = open(csv_fullpath, encoding='utf-8-sig') # this encoding is necessary to remove BOM from the beginning of CSV.
			reader = csv.reader(csvfile)
			fieldnames = next(reader) # a list of field names.

			# table name is bascially the csv file name
			table_name = os.path.split(csv_fullpath)[1]
			table_name = table_name[:-4] # remove '.csv'
			table_name = common_functions.no_special_char(table_name)
			self.logger.info("working on '%s'"%table_name)

			# the sql script for creating a new table
			create_t_sql = "CREATE TABLE %s "%table_name
			str_fieldnames = '('
			for f in fieldnames:
				str_fieldnames += f + ','
			str_fieldnames = str_fieldnames[:-1] # to remove the trailing comma

			str_fieldnames += ")"

			# we are going to sneak in a unique_id field that auto-increments as we add data.
			create_t_sql += str_fieldnames[0] + '%s integer primary key autoincrement, '%self.unique_id_fieldname + str_fieldnames[1:] + ";"


			# creating/opening the sql database and table
			con = sqlite3.connect(self.db_fullpath_new)
			cur = con.cursor()
			try:
				self.logger.debug("Creating a new table: %s"%table_name)
				# print(create_t_sql)
				cur.execute(create_t_sql)
			except:
				self.logger.info("* WARNING: Table '%s' already exists. Dropping and recreating the table."%table_name)
				cur.execute("DROP TABLE %s"%table_name)	
				cur.execute(create_t_sql)


			# inserting values
			self.logger.debug("running INSERT statement...")
			insert_sql = "INSERT INTO %s %s"%(table_name, str_fieldnames)
			row_counter = 0
			err_counter = 0
			for row in reader:
				# check if number of fieldnames matches with number of values to be inserted
				# for terraflex projects, this is most likely because lat lon values are missing.
				if len(fieldnames) < len(row):
					# this is usually the case where the FIELDNAME "latitude" or "longitude" is missing
					err_counter += 1
					continue

				if len(fieldnames) > len(row):
					# this is usually the case where the VALUE of "latitude" or "longitude" is missing
					# This can be resolved by putting 0 in the place of those missing values.
					difference = len(fieldnames) - len(row)
					blank_fill = [0 for i in range(difference)]  # [0, 0, 0] if 3 values are missing.
					row = row + blank_fill

				val = str(tuple(row))
				values_sql = " VALUES %s;"%val
				sql = insert_sql + values_sql
				# print(sql)
				cur.execute(sql)
				row_counter += 1

			if err_counter > 0:
				self.logger.info("* WARNING: Some fieldnames (such as lat long) seems to be missing in table %s. This can be caused by \
					the most recently added project or cluster survey not having gps coordinates collected."%table_name)

			# check if fieldnames include latitude and longitude
			# starting Dec 2020, there are not latitude and longitude field in terraflex connect,
			# 	instead, they have X, and Y fields. So we need to manually create latitude and longitude fields.
			# this is done by renaming attribute names. X = longitude, Y = latitude
			if 'longitude' not in fieldnames or 'latitude' not in fieldnames:
				self.logger.info("%s does not have latitude or longitude field. Looking for X & Y fields instead..."%table_name)
				for orig, new in {'X':'longitude', 'Y':'latitude'}.items():
					if orig in fieldnames:
						rename_sql = "ALTER TABLE %s RENAME COLUMN %s TO %s"%(table_name, orig, new) #eg. ALTER TABLE cluster_survey RENAME COLUMN X TO longitude
						cur.execute(rename_sql)
						self.logger.info("In the table, %s, fieldname '%s' has been renamed to '%s'"%(table_name, orig, new))
						# update fieldnames (replace X with longitude and etc.)
						for index, fieldname in enumerate(fieldnames):
							if fieldname == orig:
								fieldnames[index] = new

			# Note that starting Dec 2020, if the user have not collected lat long, the X, Y value will be blank instead of 0, 0.

			# delete test data
			if self.ignore_testdata == True:
				self.logger.debug("deleting test data records...")
				delete_sql = "DELETE FROM %s WHERE TestData = 'Yes';"%table_name
				# for example, DELETE FROM l387081_Cluster_Survey_Testing_ WHERE TestData = 'Yes';
				cur.execute(delete_sql)

				# count remaining records
				count_sql = "SELECT * FROM %s"%table_name
				count = len(cur.execute(count_sql).fetchall())
				deleted_counter = row_counter - count
				self.logger.info("Number of deleted records (test data): %s"%deleted_counter)
				row_counter = count

			self.logger.info("%s rows have been added to '%s' table in the sqlite database."%(row_counter, table_name))

			fieldnames.append(self.unique_id_fieldname)
			self.tablenames_n_rec_count[table_name] = [fieldnames,row_counter]

			con.commit()
			con.close()
			csvfile.close()



# testing
if __name__ == '__main__':

	import log
	import os
	logfile = os.path.basename(__file__) + '_deleteMeLater.txt'
	debug = True
	logger = log.logger(logfile, debug)
	logger.info('Testing %s              ############################'%os.path.basename(__file__))
	

	db_output_path = r'C:\DanielKimWork\temp'
	csvfolderpath = r'C:\Users\kimdan\OneDrive - Government of Ontario\SEM\script\data\sampledata'
	unique_id_fieldname = 'unique_id'

	test = Csv2sqlite(csvfolderpath,db_output_path,unique_id_fieldname,logger)
	# print(test.db_fullpath_new)
	# print(test.tablenames_n_rec_count)	
# {'l386505_Project_Survey': [['ProjectID', 'Date', 'DistrictName', 'ForestManagementUnit', 'Surveyors', 'Comments', 'Photos', 'longitude', 'latitude', 'hae'], 1], 'l387081_Cluster_Survey_Testing_': [['ClusterNumber',..

