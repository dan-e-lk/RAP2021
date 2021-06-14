# the purpose of this script is to add a new table to the existing sqlite database
# The new table will be created from the input shapefile that defines the boundary and other parameters.
# It will also check if the project id field contains unique project ids - throws an error if not all unique.

import os, sqlite3
from osgeo import ogr

# importing custom modules
if __name__ == '__main__':
	import common_functions
else:
	from modules import common_functions
	

class Shp2sqlite:
	"""
	Use 'run_all' method to run all the methods at once.
	"""
	def __init__(self, cfg_dict, db_filepath, tablenames_n_rec_count, logger):

		# static variables from the config file:
		self.prj_shpfile = cfg_dict['SHP']['project_shpfile']
		self.prjID_field = cfg_dict['SHP']['project_id_fieldname']
		self.new_tablename = cfg_dict['SHP']['shp2sqlite_tablename']

		# other static and non-static variables that brought into this class:
		self.db_filepath = db_filepath
		self.tablenames_n_rec_count = tablenames_n_rec_count # eg. {'l386505_Project_Survey': [['ProjectID', 'Date', 'DistrictName', 'ForestManagementUnit'],2], 'l387081_Cluster_Survey_Testing_': [['ClusterNumber',..
		self.logger = logger

		# these variables will be filled out as we go
		self.rec_count = 0
		self.attr_names = [] # eg. ['OBJECTID', 'ProjectID', 'Area_ha', 'MNRF_AsMet', 'PlotSize_m', 'YRDEP',...]
		self.shp_in_dict = [] # eg. [{'OBJECTID': 1, 'ProjectID': 'BuildingSouth', 'Area_ha': 8.41044}, {'OBJECTID': 2, 'ProjectID': 'BuildingNorth', 'Area_ha': 2.322}..]
		self.duplicates = None # this will be a list of duplicate ProjectID values if any duplicates present.


		self.logger.info('\n')
		self.logger.info('--> Running shp2sqlite module')


	def read_shpfile(self):
		"""this module turns the shapefile into a list of dictionaries.
		"""

		self.logger.debug('Running shp2sqlite.read_shpfile()')
		driver = ogr.GetDriverByName('ESRI Shapefile')
		dataSource = driver.Open(self.prj_shpfile,0) # 0 means read-only. 1 means writeable.
		
		# Check to see if shapefile is found.
		if dataSource is None:
		    self.logger.info('Could not open %s' % (self.prj_shpfile))		

		layer = dataSource.GetLayer()
		# get total number of records
		self.rec_count = layer.GetFeatureCount()
		if self.rec_count == 0:
			self.logger.info("!!! Your shapefile has zero record !!!")

		# get a list of attribute names
		layer_def = layer.GetLayerDefn()
		for n in range(layer_def.GetFieldCount()):
			field_def = layer_def.GetFieldDefn(n)
			self.attr_names.append(field_def.name)

		# grab each record in dictionary form
		temp_shp_in_dict = []
		for n in range(self.rec_count):
			temp_shp_in_dict.append(layer.GetFeature(n).items()) #eg. {'OBJECTID': 3, 'ProjectID': 'TheTrail', 'Area_ha': 29.7181,..}

		# in shp_in_dict, None objects must be converted to an empty string - so it can be entered into the sqlite
		for row in temp_shp_in_dict:
			new_row = {k:(str(v) if v != None else '') for k, v in row.items()}
			self.shp_in_dict.append(new_row) #eg. {'OBJECTID': 3, 'ProjectID': 'TheTrail', 'Area_ha': 29.7181,..}



	def check_records(self):
		""" ProjectID shouldn't have duplicate values - this module is checking that.
		"""
		self.logger.debug('Running shp2sqlite.check_records()')

		# first, make sure the project id field exists
		if self.prjID_field not in self.attr_names:
			self.logger.info('!!!! %s field does not exist in the shapefile !!!!'%self.prjID_field)

		# check if there is a duplicate
		proj_id_values_lst = [rec[self.prjID_field] for rec in self.shp_in_dict]
		proj_id_values_set = set(proj_id_values_lst)
		# if duplicate is present, len(lst) is greater than len(set)
		if len(proj_id_values_lst) > len(proj_id_values_set):
			# if duplicates found, report what the duplicates are and terminate the program!
			self.logger.info('!!!! %s field carries duplicate values !!!!'%self.prjID_field)
			self.duplicates = []
			prjid = {id:0 for id in list(proj_id_values_set)}
			for i in proj_id_values_lst:
				prjid[i] += 1
			for i, occurrance in prjid.items():
				if occurrance > 1:
					self.duplicates.append(i)
			self.logger.info('!!!! duplicates: %s !!!!'%self.duplicates)
			raise Exception("Duplicate Project IDs found. Terminating the program!")
		else:
			self.logger.debug("no duplicates found in %s field"%self.prjID_field)

		# check lat lon of the centre point
		# lat should be between 41 and 57
		# lon should be between -96 and -73
		latlon = [(float(rec['lat']),float(rec['lon'])) for rec in self.shp_in_dict]
		latlon_error = 0
		for rec in latlon:
			lat = rec[0]
			lon = rec[1]
			if 41 < lat < 57 and -96 < lon < -73:
				pass
			else:
				latlon_error += 1
		if latlon_error > 0:
			error_msg = '!!!! %s records in shapfile has wrong values in lat, lon attributes !!!!'%latlon_error
			error_msg += '\nlat should be between 41 and 57; lon should be between -96 and -73'
			self.logger.info(error_msg)
			raise Exception(error_msg)


	def to_sqlite(self):
		"""
		create a new table in the sqlite database and populate it with self.shp_in_dict that we made
		in read_shpfile module above.
		"""
		self.logger.debug('Running shp2sqlite.to_sqlite()')
		common_functions.dict_lst_to_sqlite(self.shp_in_dict, self.db_filepath, self.new_tablename, self.logger)


	def update_tablename_dict(self):
		"""
		updates self.tablenames_n_rec_count (list of attributes and records.)
		"""
		self.tablenames_n_rec_count[self.new_tablename] = [self.attr_names, self.rec_count]



	def run_all(self):
		self.read_shpfile()
		self.check_records()
		self.to_sqlite()
		self.update_tablename_dict()

# testing
if __name__ == '__main__':

	import log, common_functions
	import os
	logfile = os.path.basename(__file__) + '_deleteMeLater.txt'
	debug = True
	logger = log.logger(logfile, debug)
	logger.info('Testing %s              ############################'%os.path.basename(__file__))
	
	config_file = r'D:\ACTIVE\HomeOffice\RAP\script\SEM.cfg'
	cfg_dict = common_functions.cfg_to_dict(config_file)
	db_filepath = r'D:\ACTIVE\HomeOffice\temp\SEM_NER_200319152909.sqlite'
	tablenames_n_rec_count = {'l386505_Project_Survey': [['ProjectID', 'Date', 'DistrictName', 'ForestManagementUnit', 'Surveyors', 'Comments', 'Photos', 'longitude', 'latitude', 'hae', 'unique_id'], 1]}


	test = Shp2sqlite(cfg_dict, db_filepath, tablenames_n_rec_count, logger)
	test.run_all()
