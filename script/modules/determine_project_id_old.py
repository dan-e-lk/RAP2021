# this script runs on a shapefile.
# the shapefile (polygon) contains the information on where the projects are located, project ID, and potentially other useful information.
# Another input is the cluster points. This is stored in the sqlite database.

# The shapefile must...
# contain "ProjectID" field (can be text or integer).
# values in the "ProjectID" field must be unique (i.e. no dupilcates in ProjectID)
# be in NAD83 or WGS84 geographic coordinates. ??? may be not.
# 
# The sqlite db must have one table with name CLEARCUT_SURVEY_V2021 and another with name SHELTERWOOD_SURVEY_V2021
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
		self.geo_check_field = cfg_dict['SQLITE']['geo_check_fieldname'] # this field will be created in the sqlite database for each record in cluster table as each record gets assigned to each projectid.
		self.proj_id_field = cfg_dict['SQLITE']['fin_proj_id']
		self.proj_id_override = cfg_dict['SQLITE']['proj_id_override'] # if this field is filled out by the end-user, it should override the geomatrically found project id.
		self.unique_id_field = cfg_dict['SQLITE']['unique_id_fieldname']

		# other static and non-static variables that brought into this class:
		self.db_filepath = db_filepath
		self.tablenames_n_rec_count = tablenames_n_rec_count # eg. {'l386505_Project_Survey': [['ProjectID', 'Date', 'DistrictName', 'ForestManagementUnit'],2], 'l387081_Cluster_Survey_Testing_': [['ClusterNumber',..
		self.logger = logger

		# instance variables to be assigned as we go through each module.
		self.dataSource = None
		self.layer = None
		self.layer_featureCount = None
		self.spatialRef = None
		self.attribute_list = []  # attribute list of the input shapefile. all attribute names will be in upper class
		self.con = None # sqlite connection object
		self.cur = None
		self.clearcut_tbl_name = ''
		self.project_tbl_name = ''
		self.cluster_coords = {} # eg. {1: [48.50010352, -81.18260821], 2: [48.50010352, -81.18215905],..} where the keys are the unique ids.
		self.override_dict = {} # eg. {1: 'Use GPS', 2: 'Use GPS', 3: 'TestPrj-01',...}
		self.uniq_id_to_proj_id = {} #eg. {1: ['FUS49', 'FUS49'], ...5: ['FUS49', 'TestProj-01']}  where 'FUS49' is geometrically matching project id and 'TestProj-01' is the end-user override project id.
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

		# find the clearcut and shelterwood cluster survey tables
		clearcut_tbl_name = [i for i in self.tablenames_n_rec_count.keys() if "CLEARCUT_SURVEY_V2021" in i.upper()] # should result in one item such as ['l387081_Cluster_Survey_Testing_']
		shelterwood_tbl_name = [i for i in self.tablenames_n_rec_count.keys() if "SHELTERWOOD_SURVEY_V2021" in i.upper()]
		if len(clearcut_tbl_name) != 1:
			raise Exception("%s CLEARCUT_SURVEY_V2021 table(s) found in sqlite database. There should be 1 table."%len(clearcut_tbl_name))
		if len(shelterwood_tbl_name) != 1:
			raise Exception("%s SHELTERWOOD_SURVEY_V2021 table(s) found in sqlite database. There should be 1 table."%len(project_tbl_name))			
		self.clearcut_tbl_name = clearcut_tbl_name[0]
		self.shelterwood_tbl_name = shelterwood_tbl_name[0]

		# Create 'geo check' field and 'final project id' field for both tables
		# 'geo check' field will be used when intersecting each cluster points to the 
		# project boundaries to determine the record's project ID geographically
		for table in [self.clearcut_tbl_name, self.shelterwood_tbl_name]:
			for f in [self.geo_check_field, self.proj_id_field]:
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
		connect to the sqlite database. recognize the cluster_survey table. 
		grab coordiantes and the unique id from each record and put them in a dictionary form -> cluster_coords
		"""
		self.initiate_connection()
		self.logger.debug('grabbing coordiantes and the unique id from the cluster_survey sqlite table')


		# write sql
		# select_sql = "SELECT unique_id, latitude, longitude FROM l387081_Cluster_Survey_Testing_"
		select_sql = "SELECT %s, latitude, longitude FROM %s"%(self.unique_id_field, self.clearcut_tbl_name)
		self.logger.debug(select_sql)

		# run select query to grab coordinates and the unique ids
		# Note that starting Dec 2020, if the user have not collected lat long, the X, Y value will be blank instead of 0, 0.
		self.cluster_coords = {int(row[0]): [float(row[1] or 0),float(row[2] or 0)] for row in self.cur.execute(select_sql)} # eg. {1: [48.50010352, -81.18260821], 2: [48.50010352, -81.18215905],..} where the keys are the unique ids.

		self.logger.debug(str(self.cluster_coords))

		self.close_connection()



	def get_prjId_override_values(self):
		"""
		create unique_id: override-values dictionary (eg. {1: 'Use GPS', 2: 'Use GPS',..})
		If user specified the project id in the self.proj_id_override attribute, then this will be the final project id regardless of 
		where the data has been collected.
		"""
		self.initiate_connection()
		self.logger.debug('grabbing user-specified project id and the unique id from the cluster_survey sqlite table')


		# write sql
		# select_sql = "SELECT unique_id, latitude, longitude FROM l387081_Cluster_Survey_Testing_"
		select_sql = "SELECT %s, %s FROM %s"%(self.unique_id_field, self.proj_id_override, self.clearcut_tbl_name)
		self.logger.debug(select_sql)

		# run select query to grab coordinates and the unique ids
		self.override_dict = {int(row[0]): str(row[1]) for row in self.cur.execute(select_sql)} # eg. {1: 'Use GPS', 2: 'Use GPS', 3: 'TestPrj-01',...}
		self.logger.debug(str(self.override_dict))

		self.close_connection()



	def determine_project_id(self):
		"""
		This is where it happens! Checking if the coordinates we have in the cluster_survey are within any of the project (block) polygon shapes.
		However, if the record has self.project_id_override filled out by the end-user, that will be the project id of the record regardless of the coordinates.
		"""

		# get a list of the features in the shapefile.
		projects = [self.layer.GetFeature(i) for i in range(self.layer_featureCount)] # a list of ogr's feature objects https://gdal.org/python/osgeo.ogr.Feature-class.html
		# self.logger.debug('%s\n%s\n%s\n%s\n'%(projects[0].items(),projects[0].geometry(),projects[0].keys(),projects[0].GetField(1)))

		# iterate through cluster points
		for uniq_id, coord in self.cluster_coords.items():
			# create point geometry object
			lat = coord[0]
			lon = coord[1]
			pt = ogr.Geometry(ogr.wkbPoint)
			pt.AddPoint(lon, lat) # long, lat is apparently the default setting.

			# iterate through project polygon shapes
			matching_proj_id = None
			for proj in projects:
				# self.logger.debug('project geo = %s\npt geo = %s'%(proj.geometry(),pt))

				# Within is a method that checks if x is within y.
				if pt.Within(proj.geometry()):
					matching_proj_id = proj.items()[self.prjID_field] # proj.items() should give you something like {'Id': 2, 'ProjectID': '2'}
					break

			self.uniq_id_to_proj_id[uniq_id] = [matching_proj_id] # {1: ['FUS49'], 2: ['FUS49'],...}

			# delete the point geometry object
			del pt

		# put the override values to the uniq_id_to_proj_id dictionary only if it's been filled out by the end-user (i.e. not empty or 'Use GPS').
		for uniq_id, prj_id_list in self.uniq_id_to_proj_id.items():
			override_value = self.override_dict[uniq_id]
			if override_value == None or override_value.strip() in ['','Use GPS']:
				self.uniq_id_to_proj_id[uniq_id].append(prj_id_list[0])
			else:
				self.uniq_id_to_proj_id[uniq_id].append(self.override_dict[uniq_id]) # {1: ['FUS49', 'FUS49'], ... 5: ['FUS49', 'TestProj-01']}

		self.logger.debug('%s to %s [geographic, final]: %s'%(self.unique_id_field, self.prjID_field, self.uniq_id_to_proj_id))



	def summarize_results(self):
		"""
		report if a cluster point does not fall into any of the project polygons.
		report the number of points for each project
		"""
		total_clusters = 0
		uniq_id_of_clusters_without_projID = [] # terrible name.. but descriptive at the least.

		# get unique project ids
		projID_list = [str(i[1]) for i in self.uniq_id_to_proj_id.values() if i != None]
		projIDs_found = list(set(projID_list)) # to remove duplicate projIDs
		self.summary_dict = {prjID: 0 for prjID in projIDs_found}

		# populate summary_dict
		for uniq_id, proj_id in self.uniq_id_to_proj_id.items():
			total_clusters += 1
			if proj_id[1] == None: 
				uniq_id_of_clusters_without_projID.append(uniq_id)
			else:
				self.summary_dict[str(proj_id[1])] += 1

		num_of_clusters_without_projID = len(uniq_id_of_clusters_without_projID)
		self.summary_dict[-1] = num_of_clusters_without_projID  # eg. summary_dict = {'TestProj-01': 1, 'FUS49': 4, -1: 0}

		self.logger.debug("{Project ID (-1 indicates unassigned): Number of clusters found in that project id}:\nsummary_dict = %s"%self.summary_dict)
		if num_of_clusters_without_projID > 0:
			self.logger.info("!!!! The following cluster surveys are not located within any project boundaries !!!!")
			clus_tbl_dict_lst = common_functions.sqlite_2_dict(self.db_filepath, self.clearcut_tbl_name) # this is cluster_survey table in a list of dictionary form
			for i in uniq_id_of_clusters_without_projID:
				clus_tbl_dict = [row for row in clus_tbl_dict_lst if row[self.unique_id_field] == i][0]
				clus_num = clus_tbl_dict['ClusterNumber']
				latlon = "%s, %s"%(clus_tbl_dict['latitude'], clus_tbl_dict['longitude'])
				self.logger.info("!!!! Unique_id: %s, Cluster Num: %s, Lat Lon: %s !!!!"%(i, clus_num, latlon))


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
			geo_proj_id = '' if proj_id[0] == None else proj_id[0] # if proj_id is None, change it to ''. Otherwise, leave it as is.
			final_proj_id = '' if proj_id[1] == None else proj_id[1]

			update_sql = "UPDATE %s SET %s = '%s', %s = '%s' WHERE %s = %s"%(self.clearcut_tbl_name, self.geo_check_field, geo_proj_id, self.proj_id_field, final_proj_id, self.unique_id_field, uniq_id)
			self.logger.debug(update_sql)
			self.cur.execute(update_sql)

		self.close_connection()



	def return_updated_variables(self):
		return [self.tablenames_n_rec_count, self.uniq_id_to_proj_id, self.clearcut_tbl_name, self.project_tbl_name, self.summary_dict]



	def run_all(self):
		self.check_shpfile()
		self.create_projId_fields()
		self.get_coord_from_sqlite()
		self.get_prjId_override_values()
		self.determine_project_id()
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