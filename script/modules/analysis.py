# this module gathers and analysis whatever data we have so far 
# and outputs plot_summary, cluster_summary, and project_summary tables in the sqlite database.

import os, csv, sqlite3

# importing custom modules
if __name__ == '__main__':
	import common_functions, mymath
else:
	from modules import common_functions, mymath



class Run_analysis:
	def __init__(self, cfg_dict, db_filepath, cluster_tbl_name, project_tbl_name, spc_to_check, spc_group_dict, logger):
		# input variables
		self.proj_id = cfg_dict['SQLITE']['fin_proj_id'] # this project id attribute now exists in Cluster_Survey table.
		self.unique_id = cfg_dict['SQLITE']['unique_id_fieldname']
		self.clus_summary_tblname = cfg_dict['SQLITE']['clus_summary_tblname']
		self.proj_summary_tblname = cfg_dict['SQLITE']['proj_summary_tblname']
		self.plot_summary_tblname = cfg_dict['SQLITE']['plot_summary_tblname']		
		self.sample_max = int(cfg_dict['CALC']['max_num_of_t_per_plot'])
		self.calc_max = int(cfg_dict['CALC']['num_of_trees_4_spcomp'])
		self.num_of_plots = int(cfg_dict['CALC']['num_of_plots'])
		self.min_height = float(cfg_dict['CALC']['min_height'])
		self.max_height = float(cfg_dict['CALC']['max_height'])	# trees marked taller than max height and smaller than min height will not be counted.
		self.default_plot_area = float(cfg_dict['CALC']['default_plot_area'])
		self.db_filepath = db_filepath
		self.prj_shp_tbl_name = cfg_dict['SHP']['shp2sqlite_tablename'] # the name of the existing sqlite table that is a copy of project boundary shpfile.
		self.prj_shp_prjid_fieldname = cfg_dict['SHP']['project_id_fieldname']
		self.prj_shp_area_ha_fieldname = cfg_dict['SHP']['area_ha_fieldname']
		self.prj_shp_plotsize_fieldname = cfg_dict['SHP']['plot_size_fieldname']
		self.prj_shp_dist_fieldname = cfg_dict['SHP']['dist_fieldname']
		self.prj_shp_fmu_fieldname = cfg_dict['SHP']['fmu_fieldname']
		self.prj_shp_num_clus_fieldname = cfg_dict['SHP']['num_clus_fieldname']
		self.cluster_tbl_name = cluster_tbl_name
		self.project_tbl_name = project_tbl_name
		self.logger = logger
		self.spc_to_check = spc_to_check # eg. ['BF', 'BW', 'CE', 'LA', 'PO', 'PT', 'SB', 'SW']
		self.spc_group_dict = spc_group_dict # eg. {'BF': ['BF'], 'BW': ['BW'], 'CE': ['CE'], 'LA': ['LA'], 'PO': ['PO'], 'PT': ['PT'], 'SX': ['SB', 'SW']}

		# static variable
		self.ecosite_choices = ['dry','fresh','moist','wet']

		# instance variables to be assigned as we go through each module.
		self.cluster_in_dict = [] # [{'unique_id': 1, 'ClusterNumber': '1101', 'UnoccupiedPlot1': 'No', 'UnoccupiedreasonPlot1': '', 'Tree1SpeciesNamePlot1': 'BF (fir, balsam)', 'Tree1HeightPlot1': '1.8',...}]
		self.project_in_dict = []

		self.clus_summary_dict_lst = [] # A list of dictionaries with each dictionary representing a cluster.
		self.proj_summary_dict_lst = [] # A list of dictionaries with each dictionary representing a project.
		self.plot_summary_dict_lst = [] # A list of dictionaries with each dictionary representing a plot.

		self.logger.info("\n")
		self.logger.info("--> Running analysis module")



	def sqlite_to_dict(self):
		"""
		turn the tables in the sqlite database into lists of dictionaries.
		"""
		self.cluster_in_dict = common_functions.sqlite_2_dict(self.db_filepath, self.cluster_tbl_name) # cluster_survey table in the sqlite to a list of dictionary
		self.project_in_dict = common_functions.sqlite_2_dict(self.db_filepath, self.project_tbl_name) # project_survey table in the sqlite to a list of dictionary
		self.prj_shp_in_dict = common_functions.sqlite_2_dict(self.db_filepath, self.prj_shp_tbl_name) # projects_shp table in the sqlite to a list of dictionary

		self.logger.debug("Printing the first two SURVEYED cluster records (total %s records):\n%s\n%s\n"%(len(self.cluster_in_dict),self.cluster_in_dict[0],self.cluster_in_dict[1]))
		self.logger.debug("Printing the first SURVEYED project record (total %s records):\n%s\n"%(len(self.project_in_dict),self.project_in_dict[0]))
		self.logger.debug("Printing the first SHPFILE project record (total %s records):\n%s\n"%(len(self.prj_shp_in_dict),self.prj_shp_in_dict[0]))


	def define_attr_names(self):
		"""
		It's time to manipulate the raw data from the field.
		We will create summary tables (in the form of list of dictionaries) for each clusters and for each project
		The final form of this class will be 
		1. A list of dictionaries with each dictionary representing a cluster.
		2. A list of dictionaries with each dictionary representing a project.
		This method defines the names of the keys in those dictionaries. 
		These variables will be the attribute names of the Cluster Summary and Project Summary which will be created through out this class.
		"""
		self.ctbl_summary = 'Cluster_summary' # name of the table that will be created in the sqlite database
		self.ptbl_summary = 'Project_summary'


		# attributes of cluster_summary table
		self.c_clus_uid = 'cluster_uid'
		self.c_clus_num = 'cluster_number' # DO NOT CHANGE THIS!!!
		self.c_proj_id = 'proj_id' # DO NOT CHANGE THIS!!!
		self.c_creation_date = 'creation_date'
		self.c_all_spc_raw = 'raw_species_data' # all valid or invalid species data eg. {'P1': {'BF': 1, 'SW': 2}, 'P2': {'LA': 1}, 'P3': {'SW': 1}, 'P4': {'SW': 2}, 'P5': {'SW': 1}, 'P6': {'SW': 1}, 'P7': {'SW': 2}, 'P8': {}}
		self.c_all_spc = 'all_spc_collected' # all valid or invalid species collected and height(for effective density calc). eg. {'P1': [['BF', 5.0], ['SW', 2.0], ['SW', 2.0]], 'P2': [['LA', 3.0]], 'P3': [['SW', 1.5]], ...}
		self.c_spc = 'spc_tallest_selected' # top x (top 2) tallest trees and height(for spcomp calc) eg. [[['BW', 1.0], ['BW', 2.0]], [['SW', 2.0], ['SW', 1.5]], [['BW', 1.6]], [['SW', 2.0]], [['SW', 3.0]], [['SW', 1.5]], ...]
		self.c_spc_count = 'spc_tallest_selected_count' # top x (top 2) tallest trees (count only) eg. {'P1': {'BF': 1, 'SW': 1}, 'P2': {'LA': 1}, 'P3': {'SW': 1}, 'P4': {'SW': 2}, 'P5': {'SW': 1}, 'P6': {'SW': 1}, 'P7': {'SW': 2}, 'P8': {}}
		self.c_num_trees = 'total_num_trees' # total number of VALID trees collected (not just top x, for effective density calc) eg. 11.
		self.c_eff_dens = 'effective_density' # number of trees per hectare. (total number of trees in a cluster*10000/(8plots * 8m2)) eg. 1718.75
		self.c_invalid_spc_code = 'invalid_spc_codes' # list of invalid species codes entered by the field staff. eg. [[], [], [], ['--'], [], [], []]
		self.c_site_occ_raw = 'site_occ_data' # 0 if unoccupied. 1 if occupied. eg. {'P1': 1, 'P2': 1, 'P3': 1, 'P4': 1, 'P5': 1, 'P6': 1, 'P7': 1, 'P8': 0}
		self.c_site_occ = 'site_occ' # number of occupied plots divided by 8. eg 0.875
		self.c_site_occ_reason = 'site_occ_reason' # reason unoccupied. eg {'P1':'Slash', 'P2':'',...}
		self.c_comments = 'cluster_comments' # eg {'P1':'some comments', 'P2':'more comments',...}
		self.c_photos = 'photos' # photo url for each plot {P1:'www.photos/01|www.photos/02', P2:'',...}
		self.c_spc_comp = 'spc_comp' # number of trees for each species and their avg ht (tallest x selected only) {'BF': [1, 5.0], 'LA': [1, 3.0], 'SW': [8, 1.89]}
		self.c_spc_comp_tree_count = 'spc_comp_tree_count' # number of trees used to generate spc_comp. similar to c_num_trees but only counts top x trees per plot. eg. 10
		self.c_spc_comp_grp = 'spc_comp_grp' # number of trees for each species group (tallest x selected only) {'BF': [1, 5.0], 'LA': [1, 3.0], 'SX': [8, 1.89]}
		self.c_spc_comp_perc = 'spc_comp_perc' # same as c_spc_comp, but in percent. eg {'BF': 10.0, 'LA': 10.0, 'SW': 80.0}
		self.c_spc_comp_grp_perc = 'spc_comp_grp_perc' # {'LA': 46.7, 'SX': 53.3}
		self.c_residual = 'residual' # note that species codes are not checked here eg. {'Bw': 1, 'Pj': 2}
		self.c_ecosite = 'ecosite_moisture' # moisture and nutrient eg. 'wet'
		self.c_eco_nutri = 'ecosite_nutrient' # eg. 'very rich'
		self.c_eco_comment = 'ecosite_comment' # eg. 'this is a landing site'
		self.c_lat = 'lat' # DO NOT CHANGE THIS!!!
		self.c_lon = 'lon' # DO NOT CHANGE THIS!!!


		# attributes of project_summary table
		self.p_proj_id = 'proj_id' # DO NOT CHANGE THIS!!!
		self.p_num_clus = 'num_clusters_total' # number of clusters planned to be surveyed
		self.p_area = 'area_ha' # DO NOT CHANGE THIS!!! area in ha. this value is derived from the shapefile.
		self.p_plot_size = 'plot_size_m2' # area of each plot in m2. derived from the shp.
		self.p_spatial_fmu = 'spatial_FMU' # derived from the shp.
		self.p_spatial_dist = 'spatial_MNRF_district' # derived from the shp.
		self.p_sfl_spcomp = 'sfl_spcomp' # sfl's species comp
		self.p_sfl_so = 'sfl_so' # sfl's site occupancy
		self.p_sfl_fu = 'sfl_fu' # sfl's forest unit
		self.p_sfl_effden = 'sfl_effden' # sfl's effective density
		self.p_sfl_as_yr = 'sfl_as_yr' #sfl's assessment year

		self.p_matching_survey_rec = 'num_of_matching_survey_rec' # number of survey records found that matches with the shpfile's project ID
		self.p_is_complete = 'is_survey_complete' # yes or no or unknown  yes if p_matching_survey_rec = p_num_clus
		self.p_assessment_date = 'assessment_date' # derived from the project survey form
		self.p_assessors = 'assessors' # derived from the project survey form
		self.p_lat = 'latitude' # derived from the project survey form, if project survey form doesn't exist, derives from the project shpfile
		self.p_lon = 'longitude' # derived from the project survey form, if project survey form doesn't exist, derives from the project shpfile
		self.p_comments = 'project_comments' # derived from the project survey form
		self.p_photos = 'project_photos' # derived from the project survey form
		self.p_fmu = 'surveyor_FMU' # derived from the project survey form
		self.p_dist = 'surveyor_MNRF_district' # derived from the project survey form

		self.p_num_clus_surv = 'num_clusters_surveyed' # this is the n for site occupancy and effective density calculation
		self.p_clus_last_surv_date = 'last_cluster_survey_date'

		self.p_lst_of_clus = 'list_of_clusters'
		self.p_effect_dens_data = 'effective_density_data'
		self.p_effect_dens = 'effective_density' # 'mean', 'stdv', 'ci', 'upper_ci', 'lower_ci', 'n' values of effective density of any trees whether it's got a valid tree code or not.
		self.p_num_cl_occupied = 'num_clusters_occupied' # this is the n for species calculation
		self.p_so_data = 'site_occupancy_data'
		self.p_so = 'site_occupancy' # 'mean', 'stdv', 'ci', 'upper_ci', 'lower_ci', 'n' values of the site occupancy
		self.p_so_reason = 'site_occupancy_reason'
		self.p_spc_found = 'species_found' # a list of species found in this project
		self.p_spc_grp_found = 'species_grps_found' # a list of species groups found in this project
		self.p_spc_data = 'species_data_percent' # eg. {'SW': {'189': 70.0, '183': 85.7, '184': 72.7, '190': 80.0}, 'BF': {'189': 0, '183': 7.1, '184': 18.2, '190': 10.0},...} 
		self.p_spc_grp_data = 'species_grp_data_percent' # eg. {'BF': {'189': 0, '183': 7.1, '184': 18.2, '190': 10.0}, 'SX': {'189': 70.0, '183': 85.7, '184': 72.7, '190': 80.0},...}
		self.p_spc = 'spcomp' # 'mean', 'stdv',... for each species. eg. {'BW': {'mean': 7.42, 'stdv': 12.9916, 'ci': 16.1312, 'upper_ci': 23.5512, 'lower_ci': -8.7112, 'n': 5, 'confidence': 0.95}, 'BN': {'mean': 1.24,...
		self.p_spc_grp = 'spcomp_grp' # 'mean', 'stdv', 'ci', 'upper_ci', 'lower_ci', 'n' for each species group
		self.p_residual_data = 'residual_data'
		self.p_residual_count = 'residual_count' 
		self.p_residual_percent = 'residual_percent' # 'mean', 'stdv', 'ci', 'upper_ci', 'lower_ci', 'n' for each residual species
		self.p_residual_BA = 'residual_BA' # Basal area in m2/ha and in percent eg. {'Bw': [1.6, 0.8], 'Pj': [0.2, 0.1], 'Sb': [0.2, 0.1]}
											# note that Basal area = tree count * 2 / total number of clusters
		self.p_ecosite_data = 'ecosite_data' # {'109':['moist','rich in nutrient','some comment'], '103':['dry','',''],...}
		self.p_eco_moisture = 'ecosite_moisture' # {'moist': 8, 'wet': 2}
		self.p_analysis_comments = 'analysis_comments'
		self.p_lat = 'lat' # DO NOT CHANGE THIS!!!
		self.p_lon = 'lon' # DO NOT CHANGE THIS!!!


		# create dictionary where the keys are 'c_' variables and 'p_' variables and the values are empty for now.
		self.clus_summary_dict = {v:'' for k,v in vars(self).items() if k[:2]=='c_'} # eg. {'cluster_id': '', 'proj_id': '', 'spc_comp': '', 'spc_comp_grp': '', ...}
		self.proj_summary_dict = {v:'' for k,v in vars(self).items() if k[:2]=='p_'} # eg. {'proj_id': '', 'num_clusters_surveyed': '', 'num_clusters_occupied': '', ...}

		# also create variable name to attribute name dictionary to return.
		self.clus_summary_attr = {k:v for k,v in vars(self).items() if k[:2]=='c_'} # eg. {'c_clus_uid': 'cluster_uid', 'c_clus_num': 'cluster_number', 'c_proj_id': 'proj_id',...}
		self.proj_summary_attr = {k:v for k,v in vars(self).items() if k[:2]=='p_'}	

	def summarize_clusters(self):
		"""
		this module will go through each dictionary in self.cluster_in_dict.
		Each dictionary will be summarized and reformated to clus_summary_dict to the format much easier for further analysis.
		Dependancies - changes in the following attribute names in terraflex will break the code:
		'ClusterNumber', 
		"""

		self.logger.info('Running Summarize_clusters method')

		# loop through each cluster (i.e. each record in cluster_survey table)
		for cluster in self.cluster_in_dict:
			# record dictionary will act as a template for this cluster and the values will be filled out as we go.
			# for example, {'UnoccupiedPlot1': 'No', 'UnoccupiedreasonPlot1': '', 'Tree1SpeciesNamePlot1': 'Bf (fir, balsam)', 'Tree1HeightPlot1': '5', 'Tree2SpeciesNamePlot1': 'Sw (spruce, white)', 'Tree2HeightPlot1': '2', 'Tree3SpeciesNamePlot1': 'Sw (spruce, white)', 'Tree3HeightPlot1': '2'...}
			record = self.clus_summary_dict.copy()

			record[self.c_clus_uid] = cluster[self.unique_id] # cluster unique id
			record[self.c_clus_num] = cluster['ClusterNumber']
			record[self.c_proj_id] = cluster[self.proj_id]
			record[self.c_lat] = cluster['latitude']		
			record[self.c_lon] = cluster['longitude']
			record[self.c_creation_date] = cluster['CreationDateTime'][:10]

			c_site_occ_raw = {}
			site_occ = self.num_of_plots  # eg. 8.  Starts with total number of plots and as we find unoccup plots, deduct 1.
			site_occ_reason = {}  # this will end up being a list of all unoccup reasons eg. ['rock', 'rock', 'road'...]

			c_all_spc_raw = {} # eg. {'P1':{'BW':2, 'SW':1}, 'P2':{'MR':1}, ...}
			c_all_spc = {} # all VALID species collected and height (for effective density calc). eg. {'P1':[['BF', 5.0], ['SW', 2.0], ['SW', 2.0]], 'P2':[['SW', 1.5]], ...}
			c_num_trees = 0 # total number of VALID trees collected (for effective density calc) eg. 15.
			c_eff_dens =0 # number of VALID trees per hectare. (total number of trees in a cluster*10000/(8plots * 8m2))

			c_spc = [] # selected VALID tallestest trees for each plot will be appended to this list e.g. [[['Bf', 5.0], ['Sw', 2.0], ['Sw', 2.0]], [['La', 3.0]], [['Sw', 1.6], ['Sw', 1.9]],...]
			c_spc_count = {}
			invalid_spc_codes = []
			comments_dict = {}
			photos_dict = {}

			# looping through each plot (1-8)
			for i in range(self.num_of_plots):
				plotnum = str(i+1)
				plotname = 'P' + plotnum

				# grab comments and photos
				comments = cluster['CommentsPlot'+plotnum]
				photos = cluster['PhotosPlot'+plotnum]
				comments_dict[plotname] = comments.replace("'","")
				photos_dict[plotname] = photos
				c_site_occ_raw[plotname] = 1
				site_occ_reason[plotname] = ''
				c_all_spc_raw[plotname] = {}
				c_all_spc[plotname] = []

				# grab species
				# if the plot is unoccupied, record it and move on.
				if cluster['UnoccupiedPlot'+plotnum] == 'Yes':
					site_occ -= 1
					c_site_occ_raw[plotname] = 0
					site_occ_reason[plotname] = (cluster['UnoccupiedreasonPlot'+plotnum])
					c_spc.append([])

				# if the plot is occupied then do the following: 
				else:
					# gather all trees and heights of plot X
					trees_in_plotx = []

					# loop through max number of sample trees (6)
					for j in range(self.sample_max):
						treenum = str(j+1)
						treeXspeciesname = cluster['Tree'+treenum+'SpeciesNamePlot'+plotnum]
						if len(treeXspeciesname) >= 2: 
							treeXspeciesname = treeXspeciesname + ' '
							treeXspeciesname = treeXspeciesname[:3].strip()  # this turns 'Bf (fir, balsam)' into 'Bf'
						# for height, if the value is a number in a string, change it to a float. If that can't be done, it will be -1.
						try:
							treeXheight = float(cluster['Tree'+treenum+'HeightPlot'+plotnum])
							if self.min_height <= treeXheight <= self.max_height:
								pass
							else:
								treeXheight = -1
						except:
							treeXheight = -1
						trees_in_plotx.append([treeXspeciesname,treeXheight])

						sp_code = treeXspeciesname.upper()
						if treeXheight > 0:
							c_all_spc[plotname].append([sp_code,treeXheight])
							if sp_code in c_all_spc_raw[plotname].keys():
								c_all_spc_raw[plotname][sp_code] += 1
							else:
								c_all_spc_raw[plotname][sp_code] = 1
					# at this point, trees_in_plotx = [['La', 1.0], ['La', 2.0], ['La', 0.8], ['Sw', 3.0], ['La', -1], ['', -1]]
					# at this point, c_all_spc_raw = {{'P1':{'LA':3, 'SW':1}, 'P2':{'MR':1}, ...}}

					# let's make a list of invalid species codes we found here for reporting purpose
					invalids = [i[0].upper() for i in trees_in_plotx if len(i[0])>0 and i[0].upper() not in self.spc_to_check]
					invalid_spc_codes.append(invalids)

					# throw away species that we don't use for calculation. Also throw away species with no height measured.
					trees_in_plotx = [[i[0].upper(),i[1]] for i in trees_in_plotx if len(i[0])>1 and i[0].upper() in self.spc_to_check and i[1] != -1]
					# at this point, trees_in_plotx = [['LA', 1.0], ['LA', 2.0], ['LA', 0.8], ['SW', 3.0]]

					# c_all_spc.append(trees_in_plotx) Delete this later!!
					c_num_trees += len(trees_in_plotx)

					# if we still have more than 2 (calc_max) trees, we gotta pick the tallest 2.
					if len(trees_in_plotx) > self.calc_max:
						tallest_selected = common_functions.select_tallest_x(trees_in_plotx, self.calc_max)
					else:
						tallest_selected = trees_in_plotx
					# at this point, tallest_selected = [['SW', 3.0], ['LA', 2.0]]
					c_spc.append(tallest_selected)

			# calculate c_spc_count
			for i in range(self.num_of_plots):
				plotname = 'P' + str(i+1)
				c_spc_count[plotname] = {}
				plot_data = c_spc[i]
				if len(plot_data) > 0:
					for tree in plot_data:
						treename = tree[0]
						if treename in c_spc_count[plotname].keys():
							c_spc_count[plotname][treename] += 1
						else:
							c_spc_count[plotname][treename] = 1

			# calculating effective density
			# first we need to find the plot area (should be 8m2 by default)
			plot_area = self.default_plot_area

			# the code block below has been commented out because the plot area is always the default 8m2 for now.
			# for prj in self.prj_shp_in_dict:
			# 	# find the matching project shapefile using the project id as the key
			# 	if prj[self.prj_shp_prjid_fieldname] == cluster[self.proj_id]:
			# 		custom_plot_area = prj[self.prj_shp_plotsize_fieldname]
			# 		if custom_plot_area in ['4','8','16']: # the only option is 4, 8 and 16.
			# 			self.logger.debug("using custom plot area: %s"%custom_plot_area)
			# 			plot_area = int(custom_plot_area)

			c_eff_dens = c_num_trees*10000/(self.num_of_plots*plot_area)

			# assemble the collected information to the record dictionary.
			record[self.c_comments] = comments_dict # comments_dict looks like {'P1': 'some comments', 'P2': '',...}
			record[self.c_photos] = photos_dict
			record[self.c_site_occ_raw] = c_site_occ_raw
			record[self.c_site_occ] = float(site_occ)/self.num_of_plots # this will give you the site occupancy value between 0 and 1. eg. site_occ = 0.875, 
			record[self.c_site_occ_reason] = site_occ_reason # eg. ['Slash','Slash','Roads','Rocks']
			record[self.c_all_spc_raw] = c_all_spc_raw # eg.{'P1': {'BF': 1, 'SW': 2}, 'P2': {'LA': 1}, 'P3': {'SW': 1}, 'P4': {'SW': 2}, 'P5': {'SW': 1}, 'P6': {'SW': 1}, 'P7': {'SW': 2}, 'P8': {}}
			record[self.c_all_spc] = c_all_spc # eg. [[['Bf', 5.0], ['Sw', 2.0], ['Sw', 2.0], ['Sw', 1.9]], [['La', 3.0]], ...]
			record[self.c_spc] = c_spc # eg. [[['BF', 5.0], ['SW', 2.0]], [['LA', 3.0]], [['SW', 1.5]], [['SW', 1.6], ['SW', 1.9]], [['SW', 1.6]], [['SW', 3.0]], [['SW', 2.0], ['SW', 1.5]], []]
			record[self.c_spc_count] = c_spc_count # eg. {'P1': {'BF': 1, 'SW': 1}, 'P2': {'LA': 1}, 'P3': {'SW': 1}, 'P4': {'SW': 2}, 'P5': {'SW': 1}, 'P6': {'SW': 1}, 'P7': {'SW': 2}, 'P8': {}}
			record[self.c_num_trees] = c_num_trees # eg. 15
			record[self.c_eff_dens] = c_eff_dens
			record[self.c_invalid_spc_code] = invalid_spc_codes # eg. [[],['XY'],[],[],...]


			# we've gathered all the information we need from the cluster_survey table, but we need to summarize them.
			# remember that c_spc is a list of tallest trees with valid species codes.
			# summarizing c_spc into the following formats:
			spc_comp = {spc:[0,0] for spc in self.spc_to_check}  # {spcname:[count,avgheight]}
			spc_comp_grp = {spcgrp:[0,0] for spcgrp in self.spc_group_dict.keys()} # {spcgrpname:[count,avgheight]}
			spc_comp_tree_count = 0 

			# loop through c_spc
			for plot in c_spc:
				for tree in plot:
					spc_comp_tree_count += 1
					spc_name = tree[0]
					ht = tree[1]
					spc_comp[spc_name][0] += 1
					spc_comp[spc_name][1] += ht
					for grp, spcs in self.spc_group_dict.items():
						if spc_name in spcs:
							spc_comp_grp[grp][0] += 1
							spc_comp_grp[grp][1] += ht

			# height is sum at this point. need to convert it to avg
			# also throwing out species with count = 0
			spc_comp = {k:([v[0],round(float(v[1])/v[0],2)]) for k,v in spc_comp.items() if v[0] > 0}
			spc_comp_grp = {k:([v[0],round(float(v[1])/v[0],2)]) for k,v in spc_comp_grp.items() if v[0] > 0}
			# spc_comp looks like {'BF': [1, 5.0], 'BW': [0, -1], 'CE': [0, -1], 'LA': [1, 3.0], 'PO': [0, -1], 'PT': [0, -1], 'SB': [0, -1], 'SW': [9, 1.9]}
			# spc_comp_grp looks like {'BF': [1, 5.0], 'BW': [0, -1], 'CE': [0, -1], 'LA': [1, 3.0], 'PO': [0, -1], 'PT': [0, -1], 'SX': [9, 1.9]}


			spc_comp_perc = {k:(round(float(v[0])*100/spc_comp_tree_count,1) if spc_comp_tree_count!=0 else 0) for k,v in spc_comp.items()}
			spc_comp_grp_perc = {k:(round(float(v[0])*100/spc_comp_tree_count,1) if spc_comp_tree_count!=0 else 0) for k,v in spc_comp_grp.items()}
			# 'spc_comp_perc' looks like {'BF': 9.1, 'BW': 0.0, 'CE': 0.0, 'LA': 9.1, 'PO': 0.0, 'PT': 0.0, 'SB': 0.0, 'SW': 81.8}, 
			# 'spc_comp_grp_perc' looks like {'BF': 9.1, 'BW': 0.0, 'CE': 0.0, 'LA': 9.1, 'PO': 0.0, 'PT': 0.0, 'SX': 81.8}, 

			# assemble the collected information to the record dictionary.
			record[self.c_spc_comp] = spc_comp
			record[self.c_spc_comp_grp] = spc_comp_grp
			record[self.c_spc_comp_tree_count] = spc_comp_tree_count  # number of trees counted for spcies comp.
			record[self.c_spc_comp_perc] = spc_comp_perc
			record[self.c_spc_comp_grp_perc] = spc_comp_grp_perc


			# adding residual (overstory) and ecosite information now
			attr_lst = cluster.keys()	

			# residual:
			residual = {}  # eg. {'BW': 0, 'PJ': 0, ....}
			if cluster['Anyresidualoverstorytreesnearby'] == 'Yes':
				# find residual attributes names such as "Species1SpeciesNameResiduals" and "Species1NumberofTreesResiduals"
				spc_attrs = []
				num_attrs = []
				for attr in attr_lst:
					if attr.upper().endswith('SPECIESNAMERESIDUALS'):
						spc_attrs.append(attr)
					elif attr.upper().endswith('NUMBEROFTREESRESIDUALS'):
						num_attrs.append(attr)
				# assuming len(spc_attrs) == len(num_attrs), cause they really should be
				for i in range(len(spc_attrs)):
					spc_fullname = cluster[spc_attrs[i]] # eg. 'Bf (fir, balsam)'
					spc_num = cluster[num_attrs[i]] # eg. '3'

					if len(spc_fullname) >= 2:
						spc_fullname = spc_fullname + ' ' # adding extra space here in case the of 3 character species code.
						spc_name = spc_fullname[:3].strip().upper() # eg. spc_name = 'Bf'
					else:
						continue # move on if no species code found.
					
					try:
						spc_num = int(spc_num)
						if spc_num < 1: continue
					except ValueError:
						continue

					# once we have a valid species code and a valid species number, enter it to the residual dictionary.
					try:
						residual[spc_name] += spc_num
					except KeyError:
						residual[spc_name] = spc_num

			# ecosite values:
			ecosite = cluster['MoistureEcosite'] # moisture and nutrient eg. 'wet'
			eco_nutri = cluster['NutrientEcosite']
			eco_comment = cluster['CommentsEcosite'].replace("'","") # eg. 'this is a landing site'

			record[self.c_residual] = residual
			record[self.c_ecosite] = ecosite
			record[self.c_eco_comment] = eco_comment
			record[self.c_eco_nutri] = eco_nutri

			# all these records components are assembled and appended as a new record in the cluster summary table.
			self.clus_summary_dict_lst.append(record)

	def clus_summary_to_sqlite(self):
		""" Writing the cluster summary dictionary list to a brand new table in the sqlite database.
		"""
		common_functions.dict_lst_to_sqlite(self.clus_summary_dict_lst, self.db_filepath, self.clus_summary_tblname, self.logger)

	def summarize_projects(self):
		"""
		this module will go through each dictionary in self.prj_shp_in_dict.
		the output will be a list of dictionary that will evolve into Project Summary table in the sqlite database
		Each dictionary will be summarized and reformated to proj_summary_dict to the format much easier for further analysis.
		Note that the number of records will be equal to that of the shapefile.
		Note that the Project Survey form in the Terraflex must have the following attributes:
			ProjectID, Date, Surveyors, DistrictName, ForestManagementUnit, Comments, Photos
		"""

		self.logger.info('Running summarize_projects method')

		# loop through each cluster (i.e. each record in cluster_survey table)
		for prj in self.prj_shp_in_dict:
			# record dictionary will act as a template for this cluster and the values will be filled out as we go.
			# for example, {'UnoccupiedPlot1': 'No', 'UnoccupiedreasonPlot1': '', 'Tree1SpeciesNamePlot1': 'Bf (fir, balsam)', 'Tree1HeightPlot1': '5', 'Tree2SpeciesNamePlot1': 'Sw (spruce, white)', 'Tree2HeightPlot1': '2', 'Tree3SpeciesNamePlot1': 'Sw (spruce, white)', 'Tree3HeightPlot1': '2'...}
			record = self.proj_summary_dict.copy()
			prj_id = prj[self.prj_shp_prjid_fieldname] # project id from the shapefile
			p_analysis_comments = [] # comments will be appended here

			# copying information from the shapefile to this summary table:
			record[self.p_proj_id] = prj_id
			record[self.p_num_clus] = prj[self.prj_shp_num_clus_fieldname]
			record[self.p_area] = prj[self.prj_shp_area_ha_fieldname]
			record[self.p_plot_size] = prj[self.prj_shp_plotsize_fieldname]
			record[self.p_spatial_fmu] = prj[self.prj_shp_fmu_fieldname]
			record[self.p_spatial_dist] = prj[self.prj_shp_dist_fieldname]
			record[self.p_lat] = prj['lat']
			record[self.p_lon] = prj['lon']
			record[self.p_sfl_spcomp] = prj['SFL_SPCOMP']
			record[self.p_sfl_so] = prj['SFL_SiteOc']
			record[self.p_sfl_fu] = prj['SFL_FU']
			record[self.p_sfl_effden] = prj['SFL_EffDen']
			record[self.p_sfl_as_yr] = prj['SFL_AS_YR']


			# copying information from the Project_Survey table to this summary table:
			# need to first find the corresponding project_survey record.
			survey_rec_w_matching_projid = [i for i in self.project_in_dict if i['ProjectID'].upper() == prj_id.upper()]
			# should only be one record with this projid.
			num_found = len(survey_rec_w_matching_projid)
			record[self.p_matching_survey_rec] = num_found
			if num_found < 1:
				self.logger.debug('WARNING: no matching record for ProjectID: %s'%prj_id)
				survey_rec = None
			elif num_found > 1:
				self.logger.info('!!!! multiple matching project survey records for ProjectID = %s !!!!'%prj_id)
				survey_rec = survey_rec_w_matching_projid[0]
			else:
				survey_rec = survey_rec_w_matching_projid[0]

			if survey_rec != None:
				record[self.p_assessment_date] = survey_rec['Date'][:10]
				record[self.p_assessors] = survey_rec['Surveyors']
				# record[self.p_lat] = survey_rec['latitude']
				# record[self.p_lon] = survey_rec['longitude']
				record[self.p_comments] = survey_rec['Comments'].replace("'","")
				record[self.p_photos] = survey_rec['Photos']
				record[self.p_fmu] = survey_rec['ForestManagementUnit']
				record[self.p_dist] = survey_rec['DistrictName']


			# will loop through matching cluster_summary records and start analysing!
			cluster_rec_w_matching_projid = [i for i in self.clus_summary_dict_lst if i[self.c_proj_id] == prj_id]
			num_match = len(cluster_rec_w_matching_projid)

			# if there are matching cluster surveys but no matching project survey
			if num_found < 1 and num_match > 0:
				self.logger.info('!!!! no matching project survey record found for ProjectID = %s !!!!'%prj_id)

			# deemed complete if number of surveyed clusters equals to the total clusters
			total_num_of_clus = int(record[self.p_num_clus] or -1)
			if total_num_of_clus < 1:
				is_complete = 'unknown'
			elif num_match < total_num_of_clus:
				is_complete = 'no'
			elif num_match == total_num_of_clus:
				is_complete = 'yes'
			else:
				extra = num_match - total_num_of_clus
				is_complete = 'yes (+%s)'%extra
			num_clus_surveyed = num_match
			record[self.p_num_clus_surv] = num_clus_surveyed
			record[self.p_is_complete] = is_complete

			# loop through each cluster to grab information.
			p_lst_of_clus = []
			p_effect_dens_data = {}
			p_so_data = {}
			p_so_reason = {}
			spc_dict = {} # eg. {'109': {'BW': 30.0, 'SW': 70.0}, '108': {'BF': 18.2, 'LA': 9.1, 'SW': 72.7},...}
			spc_grp_dict = {}
			residual_dict = {} # eg. {'109': {'SW': 3, 'PW': 1}, '108': {'PB': 6, 'BF': 3},...}
			ecosite_dict = {} # eg. {'109':['moist','rich in nutrient','some comment'], '103':['dry','',''],...}
			lst_of_occupied_clus = []
			p_num_cl_occupied = 0
			clus_survey_dates = [] # eg. ['2020-06-04', '2020-04-14']
			for cluster in cluster_rec_w_matching_projid:
				cluster_num = cluster[self.c_clus_num]
				p_lst_of_clus.append(cluster_num)
				p_effect_dens_data[cluster_num] = cluster[self.c_eff_dens]
				p_so_data[cluster_num] = cluster[self.c_site_occ]
				p_so_reason[cluster_num] = cluster[self.c_site_occ_reason]
				residual_dict[cluster_num] = cluster[self.c_residual]
				ecosite_dict[cluster_num] = [cluster[self.c_ecosite],cluster[self.c_eco_nutri],cluster[self.c_eco_comment].replace("'","")]
				clus_survey_dates.append(cluster[self.c_creation_date])
				if cluster[self.c_site_occ] > 0:
					lst_of_occupied_clus.append(cluster_num)
					p_num_cl_occupied += 1
					spc_dict[cluster_num] = cluster[self.c_spc_comp_perc]
					spc_grp_dict[cluster_num] = cluster[self.c_spc_comp_grp_perc]

			# get last cluster survey date
			if len(clus_survey_dates) < 1:
				p_clus_last_surv_date = ''
			else:
				clus_survey_dates.sort()
				p_clus_last_surv_date = clus_survey_dates[-1]

			# check for duplicate cluster number
			check = mymath.check_duplicates(p_lst_of_clus) # eg. '701, 144'
			if check != None:
				warning_txt = "Duplicate cluster(s) found in project %s: %s"%(prj_id, check[1])
				p_analysis_comments.append(warning_txt)
				self.logger.info("!!!! %s !!!!"%warning_txt)

			# calculate effective density
			p_effect_dens = mymath.mean_std_ci(p_effect_dens_data) # eg. {'mean': 1979.1667, 'stdv': 1271.9428, 'ci': 1334.8221, 'upper_ci': 3313.9888, 'lower_ci': 644.3446, 'n': 6, 'confidence': 0.95}
			
			# calculate site occupancy
			p_so = mymath.mean_std_ci(p_so_data) # eg. {'mean': 0.7708, 'stdv': 0.3826, 'ci': 0.4015, 'upper_ci': 1.1723, 'lower_ci': 0.3693, 'n': 6, 'confidence': 0.95}

			# calculate spc_found and spc_grp_found
			p_spc_found = [] # eg.['CB', 'BN', 'SW', 'LA', 'BW', 'BF']
			p_spc_grp_found = [] # eg. ['CB', 'BN', 'LA', 'BW', 'SX', 'BF']
			for v in spc_dict.values():
				for i in v.keys():
					p_spc_found.append(i)
			for v in spc_grp_dict.values():
				for i in v.keys():
					p_spc_grp_found.append(i)					
			p_spc_found = list(set(p_spc_found))
			p_spc_grp_found = list(set(p_spc_grp_found))

			# calculate spc_data and spc_grp_data (n = len(lst_of_occupied_clus))
			clusters = list(set(lst_of_occupied_clus))
			clusters_dict = {clus_num:0 for clus_num in clusters} # eg. {'109':0, '103':0, '104':0,...}
			p_spc_data = {spc:clusters_dict.copy() for spc in p_spc_found} # eg. {'BF': {'109':0, '103':0}, 'BW': {'109':0, '103':0}, ...}
			p_spc_grp_data = {spc:clusters_dict.copy() for spc in p_spc_grp_found}

			for clus_num, spc_rec in spc_dict.items():
				for spc, perc in spc_rec.items():
					p_spc_data[spc][clus_num] = perc
			for clus_num, spc_rec in spc_grp_dict.items():
				for spc, perc in spc_rec.items():
					p_spc_grp_data[spc][clus_num] = perc

			# calculate p_spc and p_spc_grp (mean, stdev, etc.)
			p_spc = {spc: mymath.mean_std_ci(data) for spc, data in p_spc_data.items()}
			p_spc_grp = {spc: mymath.mean_std_ci(data) for spc, data in p_spc_grp_data.items()}


			# calcualte residuals (n = len(p_lst_of_clus))
			res_spc_found = []
			for clus, res_record in residual_dict.items():
				for spc_name in res_record.keys():
					res_spc_found.append(spc_name)
			res_spc_found = list(set(res_spc_found))
			if len(res_spc_found) > 0:
				# get residual_data		
				r_clusters = list(set(p_lst_of_clus))
				r_clusters_dict = {clus_num:0 for clus_num in r_clusters}
				p_residual_data = {spc:r_clusters_dict.copy() for spc in res_spc_found} # eg. {'PW': {'109':0, '103':0}, 'BW': {'109':0, '103':0}, ...}
				for r_clus, r_record in residual_dict.items():
					for species, num in r_record.items():
						p_residual_data[species][r_clus] = num
				
				# calculate residual_count
				p_residual_count = {spc:0 for spc in res_spc_found} # eg. {'PW': 0, 'BW': 0, 'MR': 0, ...}
				r_total_count = 0
				for r_spc, r_record in p_residual_data.items():
					for r_count in r_record.values():
						p_residual_count[r_spc] += r_count
						r_total_count += r_count
				
				# calculate residual_percent
				p_residual_percent = {spc:0 for spc in res_spc_found} # eg. {'PW': 0, 'BW': 0, 'MR': 0, ...}
				for r_spc, r_count in p_residual_count.items():
					percent = round((float(r_count)/r_total_count) * 100, 1)
					p_residual_percent[r_spc] = percent
				
				# calculate BA (total number of trees * 2 / n, where n =total number of clusters)
				p_residual_BA = {spc:0 for spc in res_spc_found} # eg. {'PW': 0, 'BW': 0, 'MR': 0, ...}	
				for r_spc, r_count in p_residual_count.items():
					ba = round(float(r_count)*2/num_clus_surveyed, 2)
					p_residual_BA[r_spc] = ba
			else:
				p_residual_data = {} # used to be 'no data'
				p_residual_count = {} # used to be 'no data'
				p_residual_percent = {} # used to be 'no data'
				p_residual_BA = {} # used to be 'no data'

			
			# calculate ecosite
			p_ecosite_data = ecosite_dict # eg. {'109':['moist','rich in nutrient','some comment'], '103':['dry','',''],...}
			if len(p_ecosite_data) > 0:
				moist = list(set([eco[0] for eco in p_ecosite_data.values()]))
				p_eco_moisture = {i:0 for i in moist} #eg. {'moist': 0, 'dry':0, ...}
				eco_count = 0
				for eco in p_ecosite_data.values():
					p_eco_moisture[eco[0]] += 1
					eco_count += 1
				# turn the count into percent
				p_eco_moisture = {k:round(float(v)*100/eco_count, 1) for k,v in p_eco_moisture.items()}
			else:
				p_eco_moisture = {} # used to be 'no data'

			# put all the calculated values to the record dictionary
			record[self.p_clus_last_surv_date] = p_clus_last_surv_date
			record[self.p_lst_of_clus] = p_lst_of_clus
			record[self.p_effect_dens_data] = p_effect_dens_data
			record[self.p_effect_dens] = p_effect_dens
			record[self.p_num_cl_occupied] = p_num_cl_occupied
			record[self.p_so_data] = p_so_data
			record[self.p_so] = p_so
			record[self.p_so_reason] = p_so_reason
			record[self.p_spc_found] = p_spc_found
			record[self.p_spc_grp_found] = p_spc_grp_found
			record[self.p_spc_data] = p_spc_data
			record[self.p_spc_grp_data] = p_spc_grp_data
			record[self.p_spc] = p_spc
			record[self.p_spc_grp] = p_spc_grp
			record[self.p_residual_data] = p_residual_data
			record[self.p_residual_count] = p_residual_count
			record[self.p_residual_percent] = p_residual_percent
			record[self.p_residual_BA] = p_residual_BA
			record[self.p_ecosite_data] = p_ecosite_data
			record[self.p_eco_moisture] = p_eco_moisture			
			record[self.p_analysis_comments] = p_analysis_comments

			# finally, append the record to the table
			self.proj_summary_dict_lst.append(record)

	def proj_summary_to_sqlite(self):
		""" Writing the cluster summary dictionary list to a brand new table in the sqlite database.
		"""
		common_functions.dict_lst_to_sqlite(self.proj_summary_dict_lst, self.db_filepath, self.proj_summary_tblname, self.logger)


	def create_plot_table(self):
		"""
		go through self.clus_summary_dict_lst again and create a plot summary table on the sqlite database.
		This table would be closest thing to the raw data collected.
		"""
		# first we need a list of all species codes found in the raw data
		# this list will be used to create attribute names of the plot_summary table.
		all_spc_codes_from_raw_data = []
		for record in self.clus_summary_dict_lst:
			for data in record[self.c_spc_count].values(): # eg. data = {'BF': 1, 'SW': 2}
				for spc_code in data.keys():
					if len(spc_code) > 0 and spc_code not in all_spc_codes_from_raw_data:
						all_spc_codes_from_raw_data.append(spc_code)
		all_spc_codes_from_raw_data.sort()

		# plot summary table will have these attributes (plus the spc codes above as attributes)
		plot_tbl_attr = ['proj_id','cluster_num','plot_num','spc_and_height','count_of_all_trees','max_num_of_trees_for_spcomp_calc']
		plot_tbl_attr_other = ['site_occupied','reason_for_unoccupancy','photos']
		plot_tbl_attr_all = plot_tbl_attr + all_spc_codes_from_raw_data + plot_tbl_attr_other

		# create a record template
		plot_summary_dict = {attr:'' for attr in plot_tbl_attr_all}

		# loop through the clusters and populate each records
		for clus_record in self.clus_summary_dict_lst:
			# loop through the number of plots we have
			for i in range(self.num_of_plots):
				plot_record = plot_summary_dict.copy()
				plotnum = str(i+1)
				plotname = 'P' + plotnum

				plot_record['proj_id'] = clus_record[self.c_proj_id]
				plot_record['cluster_num'] = clus_record[self.c_clus_num]
				plot_record['plot_num'] = plotnum
				plot_record['spc_and_height'] = clus_record[self.c_all_spc][plotname]
				plot_record['count_of_all_trees'] = len(clus_record[self.c_all_spc][plotname])
				plot_record['max_num_of_trees_for_spcomp_calc'] = self.calc_max

				# get tree counts for each species for each plot
				for spc_code in all_spc_codes_from_raw_data:
					try:
						spc_count = clus_record[self.c_spc_count][plotname][spc_code]
					except KeyError:
						spc_count = 0
					plot_record[spc_code] = spc_count

				# extra stuff
				plot_record['site_occupied'] = clus_record[self.c_site_occ_raw][plotname]
				plot_record['reason_for_unoccupancy'] = clus_record[self.c_site_occ_reason][plotname]
				plot_record['photos'] = clus_record[self.c_photos][plotname]

				self.plot_summary_dict_lst.append(plot_record)

		# create the table on the sqlite database
		common_functions.dict_lst_to_sqlite(self.plot_summary_dict_lst, self.db_filepath, self.plot_summary_tblname, self.logger)


	def create_z_tables(self):
		""" a table will be created for each project. table name example: z_BuildingNorth
		Each of these tables will carry processed data in a easily readable format.
		This will make it easy to print out on browsers and etc.
		Ingredients:
			self.proj_summary_dict_lst
			self.clus_summary_dict_lst
		"""
		
		active_projs = [record['proj_id'] for record in self.proj_summary_dict_lst if int(record['num_clusters_surveyed']) > 0]

		# create tables
		for proj in active_projs:
			proj_sum_dict = [record for record in self.proj_summary_dict_lst if record['proj_id'] == proj][0] # this returns a single record in Project_summary table in a dictionary format
			# clus_sum_dict = [record for record in self.clus_summary_dict_lst if record['proj_id'] == proj]
			lst_of_clus = sorted(list(set(proj_sum_dict[self.p_lst_of_clus]))) # eg ['179', '183', '184', '189', '190', '901']
			num_of_rows = len(lst_of_clus)
			
			# deciding the attributes for this new table
			attr = ['Cluster_Num', 'Site_Occ', 'Ef_Density', 'Moisture']
			spc_list = sorted(proj_sum_dict[self.p_spc_found]) # ['AB', 'CE', 'PB', 'PT', 'PW', 'SB', 'SW']
			attr += spc_list

			# making an empty list of dictionaries which will later turn into a table
			rec_template = {attribute:'' for attribute in attr} #eg {'Cluster Num': '', 'Site Occ': '', 'Ef Density': '', 'Moisture': '', 'AB': '',...}
			table = [] # will be filled with filled out rec_templates

			# fill out the table
			for clus in lst_of_clus:
				rec = rec_template.copy()
				rec['Cluster_Num'] = clus
				rec['Site_Occ'] = proj_sum_dict[self.p_so_data][clus]
				rec['Ef_Density'] = proj_sum_dict[self.p_effect_dens_data][clus]
				rec['Moisture'] = proj_sum_dict[self.p_ecosite_data][clus][0]
				for spc in spc_list:
					try:
						rec[spc] = proj_sum_dict[self.p_spc_data][spc][clus] # percent of that species in this cluster
					except KeyError:
						rec[spc] = 0
				table.append(rec)

			# create the name for this table.
			tablename = common_functions.create_proj_tbl_name(proj) # eg. 'Test Project1' will become 'z_Test_Project1'

			# create and populate a new sqltable for this project
			common_functions.dict_lst_to_sqlite(dict_lst=table, db_filepath=self.db_filepath, new_tablename = tablename, logger=self.logger)



	def run_all(self):
		self.sqlite_to_dict()
		self.define_attr_names()
		self.summarize_clusters()
		self.clus_summary_to_sqlite()
		self.summarize_projects()
		self.proj_summary_to_sqlite()
		self.create_plot_table()
		self.create_z_tables()





# testing
if __name__ == '__main__':

	import log
	import os
	logfile = os.path.basename(__file__) + '_deleteMeLater.txt'
	debug = True
	logger = log.logger(logfile, debug)
	logger.info('Testing %s              ############################'%os.path.basename(__file__))
	

