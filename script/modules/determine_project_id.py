# this script runs on a shapefile.
# the shapefile (polygon) contains the information on where the projects are located, project ID, and potentially other useful information.
# Another input is the cluster points. This is stored in the sqlite database.

# The shapefile must...
# contain "ProjectID" field (can be text or integer).
# values in the "ProjectID" field must be unique (i.e. no dupilcates in ProjectID)
# be in NAD83 or WGS84 geographic coordinates. ??? may be not.
# 
# Unfortunately for this 2021 season, there are more than one projectID field in the terraflex form (and thus in the sqlite table)
# 1. ProjectID: The original ProjectID field ,but this one is archieved and is NO LONGER BEING USED. Ignore this field!!
# 2. ProjectID02: This is where the field staff have entered. So USE THIS for both Clearcut and Shelterwood forms
# 3. ProjIDManualOverride: Default value = 'Use GPS' If the staff entered an incorrect ProjectID02, then the developer can manually override it using this field.
#						If the value of this field is anything other than 'Use GPS', then this will be the final ProjectID
# 4. geo_check_fieldname (geo_proj_id): for each cluster, this tool spatially determines to which project boundary the cluster point lies in.
# 5. fin_proj_id: The final projectID. Here's the hierarchy:
#			if ProjIDManualOverride != 'Use GPS':
#				fin_proj_id = ProjIDManualOverride 
#			elif geo_check_fieldname != None: 
#			 	fin_proj_id = geo_check_fieldname
#			else:
#			 	fin_proj_id = ProjectID02

# The sqlite db must have one table with name 'Clearcut_Survey_v2021' and another with name 'Shelterwood_Survey_v2021'
#
# reference: 
# https://pcjericks.github.io/py-gdalogr-cookbook/projection.html
# https://gdal.org/python/osgeo.ogr.Layer-class.html
#
# Created by Daniel Kim.


import os, sqlite3
from osgeo import ogr

# importing custom modules
if __name__ == '__main__':
	import common_functions
else:
	from modules import common_functions


class Determine_project_id:
	"""
	Use 'run_all' method to run all the methods at once.
	"""
	def __init__(self, cfg_dict, db_filepath, tablenames_n_rec_count, logger):

		# static variables from the config file:
		self.prj_shpfile = cfg_dict['SHP']['project_shpfile']
		self.prjID_field = cfg_dict['SHP']['project_id_fieldname']
		self.silvsys_fieldname = cfg_dict['SHP']['silvsys_fieldname']
		self.shp2sqlite_tablename = cfg_dict['SHP']['shp2sqlite_tablename']		
		self.geo_check_field = cfg_dict['SQLITE']['geo_check_fieldname'] # this field will be created in the sqlite database for each record in cluster table as each record gets assigned to each projectid.
		self.fin_proj_id_field = cfg_dict['SQLITE']['fin_proj_id']
		self.proj_id_override = cfg_dict['SQLITE']['proj_id_override'] # if this field is filled out by the end-user, it should override the geomatrically found project id.
		self.unique_id_field = cfg_dict['SQLITE']['unique_id_fieldname']


		# other static and non-static variables that brought into this class:
		self.db_filepath = db_filepath
		self.tablenames_n_rec_count = tablenames_n_rec_count # eg. {'Clearcut_Survey_v2021': [['ProjectID02', 'Date', 'DistrictName', 'ForestManagementUnit', ...],2], 'Shelterwood_Survey_v2021': [['ProjectID02', 'Date', 'DistrictName', 'ForestManagementUnit', ...],2]}
		self.logger = logger

		# instance variables to be assigned as we go through each module.
		self.dataSource = None
		self.layer = None
		self.layer_featureCount = None
		self.spatialRef = None
		self.attribute_list = []  # attribute list of the input shapefile. all attribute names will be in upper class
		self.con = None # sqlite connection object
		self.cur = None
		self.clearcut_tbl_name = 'Clearcut_Survey_v2021'
		self.shelterwood_tbl_name = 'Shelterwood_Survey_v2021'
		self.clearcut_coords = {} # eg. {1: [48.50010352, -81.18260821], 2: [48.50010352, -81.18215905],..} where the keys are the unique ids.
		self.shelterwood_coords = {} # eg. {1: [48.50010352, -81.18260821], 2: [48.50010352, -81.18215905],..} where the keys are the unique ids.
		self.override_dict = {} # developer's override projectid eg. {cc1: 'Use GPS', cc2: 'Use GPS',...., sh1: 'TestPrj-01',...}
		self.user_spec_proj_id_field = 'ProjectID02'
		self.user_spec_proj_id = {} # user specified projectid (ProjectID02)
		self.geo_calc_proj_id = {} # geographically matching projectid. eg. {'cc1': None, ... 'cc5': 'TIM-Gil01', 'cc6': 'TIM-Gil01', 'cc7': 'TIM-Gil01', ... 'cc11': None,...}
		self.uniq_id_to_proj_id = {} # final project ids eg. {'cc1': 'TIM-GIL01', 'cc2': 'TIM-GIL01', 'cc3': 'TIM-Gil01', 'cc4': 'TIM-Gil01', ...., 'cc10': 'NOR-HWY11-5',..., 'sh1': 'TIM-Gil01'}
		self.summary_dict = {} # eg. {'TestProj-01': 1, 'FUS49': 4, -1: 0}

		self.logger.info('\n')
		self.logger.info('--> Running determine_project_id module')



	def initiate_connection(self):
		self.logger.debug('Initiating connection with the sqlite database')
		self.con = sqlite3.connect(self.db_filepath)
		self.cur = self.con.cursor()


	def close_connection(self):
		self.logger.debug('Closing connection with the sqlite database')
		self.con.commit()
		self.con.close()


	def check_shpfile(self):
		"""
		Check...
		1. if the shapefile exists. 
		2. if the ProjectID field exists.
		3. if the shapefile is in geographic projection
		"""
		driver = ogr.GetDriverByName('ESRI Shapefile')
		self.dataSource = driver.Open(self.prj_shpfile, 0) # 0 means read-only. 1 means writeable.

		# Check to see if shapefile is found.
		if self.dataSource is None:
		    self.logger.info('Could not open %s' % (self.prj_shpfile))
		else:
		    self.logger.info('Opened %s' % (self.prj_shpfile))
		    self.layer = self.dataSource.GetLayer()
		    self.layer_featureCount = self.layer.GetFeatureCount()
		    self.logger.debug("Number of features in %s: %d" % (os.path.basename(self.prj_shpfile),self.layer_featureCount))


		# checking if the shapefile has ProjectID field
		layer_def = self.layer.GetLayerDefn()
		for n in range(layer_def.GetFieldCount()):
			field_def = layer_def.GetFieldDefn(n)
			self.attribute_list.append(field_def.name.upper())
		self.logger.debug('List of attributes found in %s:\n%s'%(os.path.basename(self.prj_shpfile),self.attribute_list))

		if self.prjID_field.upper() in self.attribute_list:
			self.logger.debug('%s field found'%self.prjID_field)
		else:
			self.logger.info('%s field NOT FOUND.'%self.prjID_field)
			raise Exception('Make sure %s field is in your shapefile!!'%self.prjID_field)


		# Check to see if shapefile is in geographic coordinates
		self.spatialRef = self.layer.GetSpatialRef()
		if not self.spatialRef.IsGeographic():
			self.logger.info('This is not geographic \nMake sure your shapefile is in WGS84')
			raise Exception('Make sure your shapefile is in WGS84 geographic coordinates')
		else:
			self.logger.debug('The shapefile is in geographic coordinates')



	def create_projId_fields(self):
		"""
		Creating geo check fields in each of the tables in sqlite database.
		The tables in the database shouldn't have this geo check field. if it does, then you can change the name of the geo field in the config file.
		"""
		self.logger.info('Adding geo check field')
		self.initiate_connection()

		# Create 'geo check' field and 'final project id' field for both tables
		# 'geo check' field will be used when intersecting each cluster points to the 
		# project boundaries to determine the record's project ID geographically
		for table in [self.clearcut_tbl_name, self.shelterwood_tbl_name]:
			for f in [self.geo_check_field, self.fin_proj_id_field]:
				if f.upper() not in [i.upper() for i in self.tablenames_n_rec_count[table][0]]:
					add_field_sql = "ALTER TABLE %s ADD %s CHAR;"%(table,f)
					self.logger.debug(add_field_sql)
					self.cur.execute(add_field_sql)
					# also update the tablenames_n_rec_count
					self.tablenames_n_rec_count[table][0].append(f)
				else:
					self.logger.info('!!%s field already exists in %s!! this may cause a problem!'%(f,table))


		self.close_connection()




	def get_coord_from_sqlite(self):
		"""
		connect to the sqlite database. 
		grab coordiantes and the unique id from each record and put them in a dictionary form -> clearcut_coords and shelterwood_coords
		"""
		self.initiate_connection()
		self.logger.debug('grabbing coordiantes and the unique id from the cluster_survey sqlite table')

		# Clearcut
		# select_sql = "SELECT unique_id, latitude, longitude FROM CLEARCUT_SURVEY_V2021"
		select_sql = "SELECT %s, latitude, longitude FROM %s"%(self.unique_id_field, self.clearcut_tbl_name)
		self.logger.debug(select_sql)
		# run select query to grab coordinates and the unique ids
		# Note that starting Dec 2020, if the user have not collected lat long, the X, Y value will be blank instead of 0, 0.
		self.clearcut_coords = {int(row[0]): [float(row[1] or 0),float(row[2] or 0)] for row in self.cur.execute(select_sql)} # eg. {1: [48.50010352, -81.18260821], 2: [48.50010352, -81.18215905],..} where the keys are the unique ids.

		# Shelterwood
		# select_sql = "SELECT unique_id, latitude, longitude FROM SHELTERWOOD_SURVEY_V2021"
		select_sql = "SELECT %s, latitude, longitude FROM %s"%(self.unique_id_field, self.shelterwood_tbl_name)
		self.logger.debug(select_sql)
		# run select query to grab coordinates and the unique ids
		# Note that starting Dec 2020, if the user have not collected lat long, the X, Y value will be blank instead of 0, 0.
		self.shelterwood_coords = {int(row[0]): [float(row[1] or 0),float(row[2] or 0)] for row in self.cur.execute(select_sql)} # eg. {1: [48.50010352, -81.18260821], 2: [48.50010352, -81.18215905],..} where the keys are the unique ids.

		self.logger.debug("Clearcut Coordinates: %s"%self.clearcut_coords)
		self.logger.debug("Shelterwood Coordinates: %s"%self.shelterwood_coords)

		self.close_connection()



	def get_prjId_override_values(self):
		"""
		create unique_id: override-values dictionary (eg. {1: 'Use GPS', 2: 'Use GPS',..})
		If user specified the project id in the self.proj_id_override attribute, then this will be the final project id regardless of 
		where the data has been collected.
		"""
		self.initiate_connection()
		self.logger.debug('grabbing proj_id_override and the unique_id from the sqlite tables')

		# Clearcut
		# select_sql = "SELECT unique_id, prj_id_override, ProjectID02 FROM CLEARCUT_SURVEY_V2021"
		select_sql = "SELECT %s, %s, %s FROM %s"%(self.unique_id_field, self.proj_id_override, self.user_spec_proj_id_field, self.clearcut_tbl_name)
		self.logger.debug(select_sql)
		# run select query
		cc_override_dict = {int(row[0]): str(row[1]) for row in self.cur.execute(select_sql)} # eg. {1: 'Use GPS', 2: 'Use GPS', 3: 'TestPrj-01',...}
		cc_user_spec_proj_id = {int(row[0]): str(row[2]) for row in self.cur.execute(select_sql)} 

		# Shelterwood
		# select_sql = "SELECT unique_id, prj_id_override, ProjectID02 FROM SHELTERWOOD_SURVEY_V2021"
		select_sql = "SELECT %s, %s, %s FROM %s"%(self.unique_id_field, self.proj_id_override, self.user_spec_proj_id_field, self.shelterwood_tbl_name)
		self.logger.debug(select_sql)
		# run select query
		sh_override_dict = {int(row[0]): str(row[1]) for row in self.cur.execute(select_sql)} # eg. {1: 'Use GPS', 2: 'Use GPS', 3: 'TestPrj-01',...}
		sh_user_spec_proj_id = {int(row[0]): str(row[2]) for row in self.cur.execute(select_sql)} 

		self.close_connection()

		# combine the two dictionaries
		for silvsys, dictionary in {'cc':cc_override_dict, 'sh':sh_override_dict}.items():
			for uniq_id, value in dictionary.items():
				self.override_dict[silvsys+str(uniq_id)] = value

		for silvsys, dictionary in {'cc':cc_user_spec_proj_id, 'sh':sh_user_spec_proj_id}.items():
			for uniq_id, value in dictionary.items():
				self.user_spec_proj_id[silvsys+str(uniq_id)] = value

		self.logger.debug("Override ProjectID Dict: %s"%self.override_dict) # eg. {cc1: 'Use GPS', cc2: 'Use GPS',...., sh1: 'TestPrj-01',...}
		self.logger.debug("User specified ProjectID Dict: %s"%self.override_dict) # eg. {'cc1': None, 'cc2': None, 'cc3': 'TIM-Gil01', 'cc4': 'TIM-Gil01', ..., 'cc20': 'NOR-HWY11-5',...}



	def determine_project_id(self):
		"""
		This is where it happens! Checking if the coordinates we have for each clusters are within any of the project (block) polygon shapes.
		if the record has self.project_id_override filled out by the end-user, that will be the project id of the record regardless of the coordinates.
		else, if the record intersects a project boundary, that will be the final project id
		else, if none of the above applies, the project id that the user has input will be the final project id.
		"""
		self.logger.info('Running determine_project_id module to geographically check the projectid')

		# get a list of the features in the shapefile.
		projects = [self.layer.GetFeature(i) for i in range(self.layer_featureCount)] # a list of ogr's feature objects https://gdal.org/python/osgeo.ogr.Feature-class.html
		# self.logger.debug('%s\n%s\n%s\n%s\n'%(projects[0].items(),projects[0].geometry(),projects[0].keys(),projects[0].GetField(1)))

		# iterate through Clearcut and Shelterwood coordinates
		for silvsys, coordinates in {'cc':self.clearcut_coords, 'sh':self.shelterwood_coords}.items():
			for uniq_id, coord in coordinates.items():
				# create point geometry object
				lat = coord[0]
				lon = coord[1]
				pt = ogr.Geometry(ogr.wkbPoint)
				pt.AddPoint(lon, lat) # long, lat is apparently the default setting.

				# iterate through project polygon shapes
				matching_proj_id = None
				for proj in projects:
					# Within is the method that checks if point a is within point b.
					if pt.Within(proj.geometry()):
						matching_proj_id = proj.items()[self.prjID_field] # proj.items() should give you something like {'Id': 2, 'ProjectID': '2'}
						# if you get error here, it's because your ProjectID field in the shp file doesn't match with the one in config file (project_id_fieldname).
						break

				self.geo_calc_proj_id[silvsys + str(uniq_id)] = matching_proj_id # {cc1: 'FUS49', cc2: None,...}

				# delete the point geometry object
				del pt

		self.logger.debug("geo_calc_proj_id = %s"%self.geo_calc_proj_id)
		# geo_calc_proj_id = {'cc1': None, ... 'cc5': 'TIM-Gil01', 'cc6': 'TIM-Gil01', 'cc7': 'TIM-Gil01', ... 'cc11': None,...}


		# now that we have all 3 ProjectID info (geo_calc_proj_id, user_spec_proj_id, and override_dict), we can decide the final projectID
		self.logger.info("Determining final ProjectID")
		for rec_num, user_proj_id in self.user_spec_proj_id.items():
			final_proj_id = user_proj_id # by default the final project id is the one user has inputted
			override_value = self.override_dict[rec_num]
			geo_calc_value = self.geo_calc_proj_id[rec_num]
			# if override project id is filled out, that's our final project id
			if override_value.strip() not in ['','Use GPS'] and override_value != user_proj_id:
				final_proj_id = override_value
				self.logger.info("Overriding ProjectID of record no. %s: orig = %s, new = %s, reason = developer override"%(rec_num, user_proj_id, override_value))
			# else if geographically calculated project id exists, that's our final project id
			elif geo_calc_value != None and geo_calc_value != user_proj_id:
				final_proj_id = geo_calc_value
				self.logger.info("Overriding ProjectID of record no. %s: orig = %s, new = %s, reason = geographic"%(rec_num, user_proj_id, geo_calc_value))
			
			# warn if no project id found
			if final_proj_id in ["", None]:
				self.logger.info("!!!! WARNING: record no. %s has no ProjectID assigned !!!!"%rec_num)
			
			# record the final project id in a new dictionary
			self.uniq_id_to_proj_id[rec_num] = final_proj_id

		self.logger.debug("Unique ID to Project ID = \n%s"%self.uniq_id_to_proj_id)
		# uniq_id_to_proj_id eg. {'cc1': 'TIM-GIL01', 'cc2': 'TIM-GIL01', 'cc3': 'TIM-Gil01', 'cc4': 'TIM-Gil01', ...., 'cc10': 'NOR-HWY11-5',..., 'sh1': 'TIM-Gil01'}

	
	def check_results(self):
		# get list of projects from the sqlite projects_shp and see if that list matches with the project ids we have in uniq_id_to_proj_id
		
		self.initiate_connection()
		self.logger.debug('Comparing the final ProjectIDs against the ProjectIDs in the shpfile')
		# select_sql = "SELECT ProjectID FROM projects_shp"
		select_sql = "SELECT %s FROM %s"%(self.prjID_field, self.shp2sqlite_tablename)
		self.logger.debug(select_sql)
		# run select query
		shp_proj_id_lst = list(set([row[0].upper() for row in self.cur.execute(select_sql)])) # eg. ['TIM-GIL01','NOR-HWY11-5','WAW-NAG-1273'...]
		self.logger.info("List of ProjectIDs found in the shpfile: %s"%shp_proj_id_lst)
		self.close_connection()

		# warn user if there's a projectID that doesn't match the one in the shpfile. (doesn't have to match case sensitivity)
		proj_id_not_in_shp = {proj_id: 0 for uniq_id, proj_id in self.uniq_id_to_proj_id.items()}
		err_count = 0
		for uniq_id, proj_id in self.uniq_id_to_proj_id.items():
			if proj_id.upper() not in shp_proj_id_lst:
				err_count += 1
				proj_id_not_in_shp[proj_id] += 1

		if err_count > 0:
			self.logger.info("!!!! WARNING: There are user-specified ProjectIDs that doesn't match the shpfile's ProjectIDs !!!!")
			for proj_id, occurrence in proj_id_not_in_shp.items():
				if occurrence > 0:
					self.logger.info("!!!! Invalid Project ID: %s,  Occurrence: %s !!!!"%(proj_id, occurrence))



	def check_silvsys(self):
		# get list of projects from the sqlite projects_shp and see if that list matches with the project ids we have in uniq_id_to_proj_id
		
		self.initiate_connection()
		self.logger.debug('Checking if the terraflex forms match the Silvsys in the shpfile')
		# select_sql = "SELECT ProjectID FROM projects_shp"
		select_sql = "SELECT %s, %s FROM %s"%(self.prjID_field, self.silvsys_fieldname, self.shp2sqlite_tablename)
		self.logger.debug(select_sql)
		# run select query
		shp_silvsys_lst = [[row[0].upper(), row[1].upper()] for row in self.cur.execute(select_sql)] # eg. [['TIM-GIL01', 'CC'], ['NOR-HWY11-5', 'CC'], ['NOR-WEYERHAUSER-6', 'SH']...]
		self.close_connection()

		# warn user if the silvicultural system of the form doesn't match with the one in the shpfile.
		silvsys_mismatch = []
		for uniq_id, proj_id in self.uniq_id_to_proj_id.items():
			field_silvsys = uniq_id[:2].upper() #eg. 'CC' or 'SH'
			for shp_record in shp_silvsys_lst:
				if shp_record[0] == proj_id.upper() and shp_record[1] != field_silvsys:
					silvsys_mismatch.append([uniq_id, proj_id, field_silvsys, shp_record[1]]) # eg. ['cc1', 'TIM-GIL01', 'CC', 'SH']
					self.logger.info("!!!! SILVSYS Mismatch found: UniqueID: %s, ProjectID: %s, Field SILVSYS: %s, Shpfile SILVSYS: %s"%(uniq_id, proj_id, field_silvsys, shp_record[1]))
					break



	def summarize_results(self):
		"""
		report if a cluster point does not fall into any of the project polygons.
		report the number of points for each project
		"""
		total_clusters = 0

		# get unique project ids
		projID_list = [i.upper() for i in self.uniq_id_to_proj_id.values() if i != None]
		projIDs_found = list(set(projID_list)) # eg. ['NOR-HWY805-7', 'WAW-NAG-790', 'NOR-PAPINEAU-3'...]
		self.summary_dict = {prjID: 0 for prjID in projIDs_found}

		# populate summary_dict
		for uniq_id, proj_id in self.uniq_id_to_proj_id.items():
			total_clusters += 1
			self.summary_dict[proj_id.upper()] += 1

		self.logger.info("ProjectID Summary: %s"%self.summary_dict)



	def populate_projID_fields(self):
		"""
		using the uniq_id_to_proj_id dictionary, populate (UPDATE) the sqlite database's geocheck field with the project ID.
		for example,
			UPDATE l387081_Cluster_Survey_Testing_
			SET geo_proj_id = 'FUS49', fin_proj_id = 'TestProj-01'
			WHERE unique_id = 1
		"""
		self.logger.info('Populating (Updating) SQLite geo_check field with ProjectIDs')
		self.initiate_connection()

		for uniq_id, proj_id in self.uniq_id_to_proj_id.items():
			silvsys = uniq_id[:2].upper()
			geo_proj_id = '' if self.geo_calc_proj_id[uniq_id] == None else self.geo_calc_proj_id[uniq_id]
			final_proj_id = '' if proj_id == None else proj_id

			if silvsys == "CC":
				# eg. UPDATE Clearcut_Survey_v2021 SET geo_proj_id = 'value', fin_proj_id = 'value' WHERE unique_id = 12
				update_sql = "UPDATE %s SET %s = '%s', %s = '%s' WHERE %s = %s"%(self.clearcut_tbl_name, self.geo_check_field, 
					geo_proj_id, self.fin_proj_id_field, final_proj_id, self.unique_id_field, uniq_id[2:])
				self.logger.debug(update_sql)
				self.cur.execute(update_sql)
			else:
				# eg. UPDATE Shelterwood_Survey_v2021 SET geo_proj_id = 'value', fin_proj_id = 'value' WHERE unique_id = 12
				update_sql = "UPDATE %s SET %s = '%s', %s = '%s' WHERE %s = %s"%(self.shelterwood_tbl_name, self.geo_check_field, 
					geo_proj_id, self.fin_proj_id_field, final_proj_id, self.unique_id_field, uniq_id[2:])
				self.logger.debug(update_sql)
				self.cur.execute(update_sql)				

		self.close_connection()



	def return_updated_variables(self):
		return [self.tablenames_n_rec_count, self.uniq_id_to_proj_id, self.clearcut_tbl_name, self.shelterwood_tbl_name, self.summary_dict]



	def run_all(self):
		self.check_shpfile()
		self.create_projId_fields()
		self.get_coord_from_sqlite()
		self.get_prjId_override_values()
		self.determine_project_id()
		self.check_results()
		self.check_silvsys()
		self.summarize_results()
		self.populate_projID_fields()






# testing
if __name__ == '__main__':

	import log
	import os
	logfile = os.path.basename(__file__) + '_deleteMeLater.txt'
	debug = True
	logger = log.logger(logfile, debug)
	logger.info('Testing %s              ############################'%os.path.basename(__file__))


	# variables:
	# project_shapefile = r'C:\Users\kimdan\ONEDRI~1\SEM\PARKIN~1\shp\PARKIN~2.SHP'
	# project_shapefile = r'C:\Users\kimdan\ONEDRI~1\SEM\PARKIN~1\shp\PARKIN~1.SHP'

	cfg_dict = {'SHP': {'project_shpfile': r'C:\Users\kimdan\OneDrive - Government of Ontario\SEM\parkinglot_testing\shp\parkinglot_random_shape.shp',
         				'projectid_fieldname': 'ProjectID'},
 				'SQLITE': {'geo_check_fieldname': 'geo_check',
            			'unique_id_fieldname': 'unique_id'}
            	}


	db_filepath = r'C:\DanielKimWork\temp\SEM_NER_200129175638.sqlite'
	tablenames_n_rec_count = {'l386505_Project_Survey': [['geo_check','ProjectID', 'Date', 'DistrictName', 'ForestManagementUnit', 'Surveyors', 'Comments', 'Photos', 'longitude', 'latitude', 'hae', 'unique_id'], 1], 'l387081_Cluster_Survey_Testing_': [['geo_check','ClusterNumber', 'UnoccupiedPlot1', 'UnoccupiedreasonPlot1', 'Tree1SpeciesNamePlot1', 'Tree1HeightPlot1', 'Tree2SpeciesNamePlot1', 'Tree2HeightPlot1', 'Tree3SpeciesNamePlot1', 'Tree3HeightPlot1', 'Tree4SpeciesNamePlot1', 'Tree4HeightPlot1', 'Tree5SpeciesNamePlot1', 'Tree5HeightPlot1', 'Tree6SpeciesNamePlot1', 'Tree6HeightPlot1', 'CommentsPlot1', 'PhotosPlot1', 'UnoccupiedPlot2', 'UnoccupiedreasonPlot2', 'Tree1SpeciesNamePlot2', 'Tree1HeightPlot2', 'Tree2SpeciesNamePlot2', 'Tree2HeightPlot2', 'Tree3SpeciesNamePlot2', 'Tree3HeightPlot2', 'Tree4SpeciesNamePlot2', 'Tree4HeightPlot2', 'Tree5SpeciesNamePlot2', 'Tree5HeightPlot2', 'Tree6SpeciesNamePlot2', 'Tree6HeightPlot2', 'CommentsPlot2', 'PhotosPlot2', 'UnoccupiedPlot3', 'UnoccupiedreasonPlot3', 'Tree1SpeciesNamePlot3', 'Tree1HeightPlot3', 'Tree2SpeciesNamePlot3', 'Tree2HeightPlot3', 'Tree3SpeciesNamePlot3', 'Tree3HeightPlot3', 'Tree4SpeciesNamePlot3', 'Tree4HeightPlot3', 'Tree5SpeciesNamePlot3', 'Tree5HeightPlot3', 'Tree6SpeciesNamePlot3', 'Tree6HeightPlot3', 'CommentsPlot3', 'PhotosPlot3', 'UnoccupiedPlot4', 'UnoccupiedreasonPlot4', 'Tree1SpeciesNamePlot4', 'Tree1HeightPlot4', 'Tree2SpeciesNamePlot4', 'Tree2HeightPlot4', 'Tree3SpeciesNamePlot4', 'Tree3HeightPlot4', 'Tree4SpeciesNamePlot4', 'Tree4HeightPlot4', 'Tree5SpeciesNamePlot4', 'Tree5HeightPlot4', 'Tree6SpeciesNamePlot4', 'Tree6HeightPlot4', 'CommentsPlot4', 'PhotosPlot4', 'Species1SpeciesName', 'Species1SizeClass', 'Species1NumberofTrees', 'Species1Quality', 'ShelterwoodLightLevel', 'MidStoryInterference', 'CrownClosureEstimate', 'OverstoryPhotos', 'UnoccupiedPlot5', 'UnoccupiedreasonPlot5', 'Tree1SpeciesNamePlot5', 'Tree1HeightPlot5', 'Tree2SpeciesNamePlot5', 'Tree2HeightPlot5', 'Tree3SpeciesNamePlot5', 'Tree3HeightPlot5', 'Tree4SpeciesNamePlot5', 'Tree4HeightPlot5', 'Tree5SpeciesNamePlot5', 'Tree5HeightPlot5', 'Tree6SpeciesNamePlot5', 'Tree6HeightPlot5', 'CommentsPlot5', 'PhotosPlot5', 'UnoccupiedPlot6', 'UnoccupiedreasonPlot6', 'Tree1SpeciesNamePlot6', 'Tree1HeightPlot6', 'Tree2SpeciesNamePlot6', 'Tree2HeightPlot6', 'Tree3SpeciesNamePlot6', 'Tree3HeightPlot6', 'Tree4SpeciesNamePlot6', 'Tree4HeightPlot6', 'Tree5SpeciesNamePlot6', 'Tree5HeightPlot6', 'Tree6SpeciesNamePlot6', 'Tree6HeightPlot6', 'CommentsPlot6', 'PhotosPlot6', 'UnoccupiedPlot7', 'UnoccupiedreasonPlot7', 'Tree1SpeciesNamePlot7', 'Tree1HeightPlot7', 'Tree2SpeciesNamePlot7', 'Tree2HeightPlot7', 'Tree3SpeciesNamePlot7', 'Tree3HeightPlot7', 'Tree4SpeciesNamePlot7', 'Tree4HeightPlot7', 'Tree5SpeciesNamePlot7', 'Tree5HeightPlot7', 'Tree6SpeciesNamePlot7', 'Tree6HeightPlot7', 'CommentsPlot7', 'PhotosPlot7', 'UnoccupiedPlot8', 'UnoccupiedreasonPlot8', 'Tree1SpeciesNamePlot8', 'Tree1HeightPlot8', 'Tree2SpeciesNamePlot8', 'Tree2HeightPlot8', 'Tree3SpeciesNamePlot8', 'Tree3HeightPlot8', 'Tree4SpeciesNamePlot8', 'Tree4HeightPlot8', 'Tree5SpeciesNamePlot8', 'Tree5HeightPlot8', 'Tree6SpeciesNamePlot8', 'Tree6HeightPlot8', 'CommentsPlot8', 'PhotosPlot8', 'CollectedBy', 'CreationDateTime', 'UpdateDateTime', 'longitude', 'latitude', 'hae', 'unique_id'], 16]}


	go = Determine_project_id(cfg_dict, db_filepath, tablenames_n_rec_count, logger)
	go.run_all()