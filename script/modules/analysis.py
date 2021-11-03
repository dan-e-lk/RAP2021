# this module gathers and analysis whatever data we have so far 
# and outputs plot_summary, cluster_summary, and project_summary tables in the sqlite database.

import os, csv, sqlite3, shutil

# importing custom modules
if __name__ == '__main__':
	import common_functions, mymath
else:
	from modules import common_functions, mymath



class Run_analysis:
	def __init__(self, cfg_dict, db_filepath, clearcut_tbl_name, shelterwood_tbl_name, spc_to_check, spc_group_dict, logger):
		# input variables
		self.cfg_dict = cfg_dict
		self.fin_proj_id = cfg_dict['SQLITE']['fin_proj_id'] # this project id attribute now exists in Cluster_Survey table.
		self.unique_id = cfg_dict['SQLITE']['unique_id_fieldname']
		self.clus_summary_tblname = cfg_dict['SQLITE']['clus_summary_tblname']
		self.proj_summary_tblname = cfg_dict['SQLITE']['proj_summary_tblname']
		self.plot_summary_tblname = cfg_dict['SQLITE']['plot_summary_tblname']		
		self.max_num_of_t_per_sqm = float(cfg_dict['CALC']['max_num_of_t_per_sqm']) # 0.5 
		# self.calc_max = int(cfg_dict['CALC']['num_of_trees_4_spcomp'])
		self.num_of_plots = int(cfg_dict['CALC']['num_of_plots'])
		self.clearcut_plot_area = 8 # sq m
		self.shelterwood_plot_area = 16 # sq m
		self.db_filepath = db_filepath
		self.prj_shp_tbl_name = cfg_dict['SHP']['shp2sqlite_tablename'] # the name of the existing sqlite table that is a copy of project boundary shpfile.
		self.prj_shp_prjid_fieldname = cfg_dict['SHP']['project_id_fieldname'].upper()
		# self.prj_shp_area_ha_fieldname = cfg_dict['SHP']['area_ha_fieldname'].upper()
		# self.prj_shp_dist_fieldname = cfg_dict['SHP']['dist_fieldname'].upper()
		# self.prj_shp_fmu_fieldname = cfg_dict['SHP']['fmu_fieldname'].upper()
		# self.prj_shp_num_clus_fieldname = cfg_dict['SHP']['num_clus_fieldname'].upper()
		self.clearcut_tbl_name = clearcut_tbl_name
		self.shelterwood_tbl_name = shelterwood_tbl_name
		self.logger = logger
		self.spc_to_check = spc_to_check # eg. ['BF', 'BW', 'CE', 'LA', 'PO', 'PT', 'SB', 'SW']
		self.spc_group_dict = spc_group_dict # eg. {'BF': ['BF'], 'BW': ['BW'], 'CE': ['CE'], 'LA': ['LA'], 'PO': ['PO'], 'PT': ['PT'], 'SX': ['SB', 'SW']}

		# static variable
		self.ecosite_choices = ['dry','fresh','moist','wet', 'not applicable']

		# instance variables to be assigned as we go through each module.
		self.cc_cluster_in_dict = [] # eg. [{'unique_id': 1, 'ClusterNumber': '1101', 'UnoccupiedPlot1': 'No', 'UnoccupiedreasonPlot1': '', 'Tree1SpeciesNamePlot1': 'BF (fir, balsam)', 'Tree1HeightPlot1': '1.8',...}]
		self.sh_cluster_in_dict = [] # both cc_cluster_in_dict and sh_cluster_in_dict will be summarized into clus_summary_dict_lst

		self.clus_summary_dict_lst = [] # A list of dictionaries with each dictionary representing a cluster.
		self.proj_summary_dict_lst = [] # A list of dictionaries with each dictionary representing a project.
		self.plot_summary_dict_lst_cc = [] # A list of dictionaries with each dictionary representing a plot.
		self.plot_summary_dict_lst_sh = [] # A list of dictionaries with each dictionary representing a plot.

		self.logger.info("\n")
		self.logger.info("--> Running analysis module")



	def sqlite_to_dict(self):
		"""
		turn the tables in the sqlite database into lists of dictionaries.
		"""
		self.cc_cluster_in_dict = common_functions.sqlite_2_dict(self.db_filepath, self.clearcut_tbl_name) # clearcut_survey table in the sqlite to a list of dictionary
		self.sh_cluster_in_dict = common_functions.sqlite_2_dict(self.db_filepath, self.shelterwood_tbl_name) # shelterwood_survey table in the sqlite to a list of dictionary
		self.prj_shp_in_dict = common_functions.sqlite_2_dict(self.db_filepath, self.prj_shp_tbl_name) # projects_shp table in the sqlite to a list of dictionary

		if len(self.cc_cluster_in_dict) > 1:
			self.logger.debug("Printing the first SURVEYED clearcut record (total %s records):\n%s\n"%(len(self.cc_cluster_in_dict),self.cc_cluster_in_dict[0]))
		if len(self.sh_cluster_in_dict) > 1:
			self.logger.debug("Printing the first SURVEYED shelterwood record (total %s records):\n%s\n"%(len(self.sh_cluster_in_dict),self.sh_cluster_in_dict[0]))
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
		self.c_silvsys = 'silvsys' # CC or SH as collected

		self.c_spc_count = 'spc_count' # eg. {'P1':[{'BW':2, 'SW':1}, {}], 'P2': None, 'P3': [{'SW': 1}, {'SW': 2}],...}
		self.c_num_trees = 'total_num_trees' # total number of VALID trees collected eg. 11.
		self.c_eff_dens = 'effective_density' # number of trees per hectare. (total number of trees in a cluster*10000/(8plots * 8m2)) eg. 1718.75
		self.c_invalid_spc_code = 'invalid_spc_codes' # list of invalid species codes entered by the field staff. eg. [[], [], [], ['--'], [], [], []]
		self.c_site_occ_raw = 'site_occ_data' # 0 if unoccupied. 1 if occupied. eg. {'P1': 1, 'P2': 1, 'P3': 1, 'P4': 1, 'P5': 1, 'P6': 1, 'P7': 1, 'P8': 0}
		self.c_site_occ = 'site_occ' # number of occupied plots divided by 8. eg 0.875
		self.c_site_occ_reason = 'site_occ_reason' # reason unoccupied. eg {'P1':'Slash', 'P2':'',...}
		self.c_comments = 'cluster_comments' # eg {'cluster':'some comments', 'P1':'some comments', 'P2':'more comments',...}
		self.c_photos = 'photos' # photo url for each plot {'cluster':'www.photos/03','P1':'www.photos/01|www.photos/02', 'P2':'',...}
		self.c_spc_comp = 'spc_comp' # number of trees for each species {BF': 1, 'LA': 1, 'SW': 8}
		self.c_spc_comp_grp = 'spc_comp_grp' # number of trees for each species group {'BF': 1, 'LA': 1, 'SX': 8}
		self.c_spc_comp_perc = 'spc_comp_perc' # same as c_spc_comp, but in percent. eg {'BF': 10.0, 'LA': 10.0, 'SW': 80.0}
		self.c_spc_comp_grp_perc = 'spc_comp_grp_perc' # {'LA': 46.7, 'SX': 53.3}

		self.c_ecosite = 'ecosite_moisture' # moisture and nutrient eg. 'wet'
		self.c_eco_nutri = 'ecosite_nutrient' # eg. 'very rich'
		self.c_eco_comment = 'ecosite_comment' # eg. 'this is a landing site'
		self.c_lat = 'lat' # DO NOT CHANGE THIS!!!
		self.c_lon = 'lon' # DO NOT CHANGE THIS!!!

		self.c_local_sync_photopath = 'local_sync_photopath' # new photo path. C drive, but sync'ed to sharepoint. eg {'cluster':['C:/Users/kimdan/Government of Ontario/Regeneration Assessment Program - RAP Picture Library/03.jpg','C:/Users/kimdan/Government of Ontario/Regeneration Assessment Program - RAP Picture Library/04.jpg'],'P1':[], 'P2':[],...}
		self.c_sharepoint_photopath = 'sharepoint_photopath' # final photo path (same format as above)


		# attributes of project_summary table
		self.p_proj_id = 'proj_id' # DO NOT CHANGE THIS!!!
		self.p_num_clus = 'num_clusters_total' # number of clusters planned to be surveyed as specified in the shp file
		self.p_silvsys = 'silvsys'  # CC or SH, as collected
		self.p_area = 'area_ha' # DO NOT CHANGE THIS!!! area in ha. this value is derived from the shapefile.
		self.p_plot_size = 'plot_size_m2' # area of each plot in m2. CC = 8 sq m, SH = 16 sq m
		self.p_spatial_fmu = 'spatial_FMU' # derived from the shp.
		self.p_spatial_dist = 'spatial_MNRF_district' # derived from the shp.
		self.p_lat = 'lat' # DO NOT CHANGE THIS!!!
		self.p_lon = 'lon' # DO NOT CHANGE THIS!!!

		self.p_yrdep = 'YRDEP' # year of last depletion, derived from the shp
		self.p_depfu = 'depletion_fu' # Forest unit at the time of depletion, derived from the shp
		self.p_yrorg = 'YRORG' # derived from the shp
		self.p_sgr = 'SGR' # derived from the shp
		self.p_targetfu = 'target_fu' # derived from the shp
		self.p_targetspc = 'target_spc' # derived from the shp
		self.p_targetso = 'target_so' # derived from the shp

		self.p_sfl_as_yr = 'sfl_as_yr' #sfl's assessment year
		self.p_sfl_as_method = 'sfl_as_method' #sfl's assessment method (ground/aerial)
		self.p_sfl_spcomp = 'sfl_spcomp' # sfl's species comp
		self.p_sfl_so = 'sfl_so' # sfl's site occupancy
		self.p_sfl_fu = 'sfl_fu' # sfl's forest unit
		self.p_sfl_effden = 'sfl_effden' # sfl's effective density

		self.p_num_clus_surv = 'num_clusters_surveyed' # DO NOT CHANGE. number of clusters surveyed for each ProjectID 
		self.p_lst_of_clus = 'list_of_clusters'		
		self.p_is_complete = 'is_survey_complete' # yes or no or unknown  yes if p_matching_survey_rec >= p_num_clus
		self.p_assess_start_date = 'assess_start_date' # earliest survey form creation date
		self.p_assess_last_date = 'assess_last_date' # latest survey form creation date
		self.p_assessors = 'assessors'
		self.p_fmu = 'surveyor_FMU' # derived from the survey form
		self.p_dist = 'surveyor_MNRF_district' # derived from the survey form
		self.p_comments = 'all_comments' # all comments combined

		self.p_effect_dens_data = 'effective_density_data' # eff density, raw data in dict. eg. {'109': 1225, '108': 1375,...}
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

		self.p_ecosite_data = 'ecosite_data' # {'109':['moist','rich in nutrient','some comment'], '103':['dry','',''],...}
		self.p_eco_moisture = 'ecosite_moisture' # {'moist': 8, 'wet': 2}
		self.p_analysis_comments = 'analysis_comments' # auto generated comments and warnings occurred during the analysis of this project boundary


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

		# loop through each cluster (i.e. each record in clearcut_survey and shelterwood_survey table)
		for silvsys, cluster_in_dict in {'CC': self.cc_cluster_in_dict, 'SH': self.sh_cluster_in_dict}.items():
			for cluster in cluster_in_dict:
				# record dictionary will act as a template for this cluster and the values will be filled out as we go.
				# for example, {'UnoccupiedPlot1': 'No', 'UnoccupiedreasonPlot1': '', 'Tree1SpeciesNamePlot1': 'Bf (fir, balsam)', 'Tree1HeightPlot1': '5', 'Tree2SpeciesNamePlot1': 'Sw (spruce, white)', 'Tree2HeightPlot1': '2', 'Tree3SpeciesNamePlot1': 'Sw (spruce, white)', 'Tree3HeightPlot1': '2'...}
				self.logger.debug("\tWorking on Proj[%s] Clus[%s]..."%(cluster[self.fin_proj_id], cluster['ClusterNumber']))
				record = self.clus_summary_dict.copy() # each record is one cluster in a dictionary form

				record[self.c_clus_uid] = cluster[self.unique_id] # cluster unique id
				record[self.c_clus_num] = cluster['ClusterNumber']
				record[self.c_proj_id] = cluster[self.fin_proj_id] # fin_proj_id
				record[self.c_lat] = cluster['latitude']
				record[self.c_lon] = cluster['longitude']
				record[self.c_creation_date] = cluster['CreationDateTime'][:10] # eg. '2021-09-09'
				record[self.c_silvsys] = silvsys # 'CC' or 'SH'


				c_site_occ_raw = {} # {'P1':1, 'P2':0, ...} 1 if occupied, 0 if unoccupied
				site_occ = self.num_of_plots  # eg. 8.  Starts with total number of plots and as we find unoccup plots, deduct 1.
				site_occ_reason = {}  # this will end up being a list of all unoccup reasons eg. {'P1':'', 'P2': 'Road', ...}

				# c_all_spc_raw = {} # num of spc for both 8m2 and 16m2. eg. {'P1':{'BW':2, 'SW':1}, 'P2':{'MR':1} ...} 
				# c_all_spc = {} # all VALID species collected and height (for effective density calc). eg. {'P1':[['BF', 5.0], ['SW', 2.0], ['SW', 2.0]], 'P2':[['SW', 1.5]], ...}
				c_num_trees = 0 # total number of trees collected (for effective density calc) eg. 15.
				c_eff_dens =0 # number of trees per hectare. (total number of trees in a cluster*10000/(8plots * 8m2))

				# c_spc = [] # selected VALID tallestest trees for each plot will be appended to this list e.g. [[['Bf', 5.0], ['Sw', 2.0], ['Sw', 2.0]], [['La', 3.0]], [['Sw', 1.6], ['Sw', 1.9]],...]
				c_spc_count = {} # eg. {'P1':[{'BW':2, 'SW':1}, {}], 'P2': None ...} 
				invalid_spc_codes = [] 
				comments_dict = {'cluster':cluster['GeneralComment'], 'ecosite':cluster['CommentsEcosite']} # eg. {'cluster': '7 staff', 'P1': 'all residual trees', ... }
				photos_dict = {'cluster':cluster['ClusterPhoto']} # eg. {'cluster':'www.photos/03','P1':'www.photos/01|www.photos/02', 'P2':'',...}

				# looping through each plot (1-8)
				for i in range(self.num_of_plots):
					plotnum = str(i+1)
					plotname = 'P' + plotnum

					# grab comments and photos
					comments = cluster['CommentsPlot'+plotnum]
					photos = cluster['PhotosPlot'+plotnum]
					comments_dict[plotname] = comments.replace("'","") # having apostrophe causes trouble later
					photos_dict[plotname] = photos
					c_site_occ_raw[plotname] = 1
					site_occ_reason[plotname] = ''
					p_num_trees = 0 # total number of trees collected for each plot

					# grab species
					# if the plot is unoccupied, record it and move on.
					if cluster['UnoccupiedPlot'+plotnum] == 'Yes':
						site_occ -= 1
						c_site_occ_raw[plotname] = 0
						site_occ_reason[plotname] = (cluster['UnoccupiedreasonPlot'+plotnum])
						c_spc_count[plotname] = None # eg. {'P2': None}

					# if the plot is occupied then do the following:
					# the goal is to populate c_spc_count # eg. {'P1':[{'BW':2, 'SW':1}, {}], 'P2': None ...} 
					else:
						if silvsys == 'CC':
							# Species1SpeciesNamePlot1 ~ Species4SpeciesNamePlot8  # up to 4 species, 8 plots
							# Species1NumberofTreesPlot1 ~ Species4NumberofTreesPlot8 # x number of trees per species
							# loop through 1-4
							spc_dict_8m2 = {} # eg. {'BW':2, 'SW':1}
							spc_dict_16m2 = {} # only for sh
							for spc_num in range(1,5):
								spc_name = cluster['Species'+str(spc_num)+'SpeciesNamePlot'+plotnum] # eg. 'Bf (fir, balsam)' or ''
								if len(spc_name) >= 2:
									spc_name = spc_name + ' ' # some species codes are 3 letters, so this is necessary
									spc_code = spc_name[:3].strip().upper()  # this turns 'Bf (fir, balsam)' into 'BF'
									if spc_code not in self.spc_to_check:
										self.logger.info("Invalid Species Name Found (and will not be counted): PrjID=%s, Clus=%s, SpeciesName=%s"%(cluster[self.fin_proj_id], cluster['ClusterNumber'],spc_name))
										invalid_spc_codes.append(spc_name)
										continue # move on to the next species without running any of the scripts below within this for loop
								else:
									spc_code = None
									continue # move on to the next species

								# below will run only if we have a species code such as "Bf"
								spc_count_raw = cluster['Species'+str(spc_num)+'NumberofTreesPlot'+plotnum] # eg. '2' or ''
								if spc_count_raw in ['0', '', None]:
									continue # move on to the next species
								else:
									spc_count = int(spc_count_raw)
									c_num_trees += spc_count
									p_num_trees += spc_count
									if spc_code in spc_dict_8m2.keys():
										spc_dict_8m2[spc_code] += spc_count
									else:
										spc_dict_8m2[spc_code] = spc_count # eg. {'BW':2}
							# sum up
							c_spc_count[plotname] = [spc_dict_8m2, spc_dict_16m2] # eg. {'P1':[{'BW':2, 'SW':1}, {}] }

						elif silvsys == 'SH':
							# Species1SpeciesNamePlot1 ~ Species6SpeciesNamePlot8  # species 1~3: 8m2 plot.  species 4~6: 16m2 plot
							# Species1NumberofTreesPlot1 ~ Species6NumberofTreesPlot8 # x number of trees per species
							# loop through 1-6
							spc_dict_8m2 = {} # eg. {'BW':2, 'SW':1}
							spc_dict_16m2 = {} # only for sh
							for spc_num in range(1,7):
								spc_name = cluster['Species'+str(spc_num)+'SpeciesNamePlot'+plotnum] # eg. 'Bf (fir, balsam)' or ''
								if len(spc_name) >= 2:
									spc_name = spc_name + ' ' # some species codes are 3 letters, so this is necessary
									spc_code = spc_name[:3].strip().upper()  # this turns 'Bf (fir, balsam)' into 'BF'
									if spc_code not in self.spc_to_check:
										self.logger.info("!!!! Invalid Species Name Found (and will not be counted): PrjID=%s, Clus=%s, SpeciesName=%s"%(cluster[self.proj_id], cluster['ClusterNumber'],spc_name))
										invalid_spc_codes.append(spc_name)
										continue # move on to the next species without running any of the scripts below within this for loop
								else:
									spc_code = None
									continue # move on to the next species

								# below will run only if we have a species code such as "Bf"
								spc_count_raw = cluster['Species'+str(spc_num)+'NumberofTreesPlot'+plotnum] # eg. '2' or ''
								if spc_count_raw in ['0', '', None]:
									continue # move on to the next species
								else:
									spc_count = int(spc_count_raw)
									c_num_trees += spc_count
									p_num_trees += spc_count
									# first 3 species are for 8m2
									if spc_num in [1,2,3]:
										if spc_code in spc_dict_8m2.keys():
											spc_dict_8m2[spc_code] += spc_count
										else:
											spc_dict_8m2[spc_code] = spc_count # eg. {'BW':2}
									# the next 3 species are for 16m2
									elif spc_num in [4,5,6]:
										if spc_code in spc_dict_16m2.keys():
											spc_dict_16m2[spc_code] += spc_count
										else:
											spc_dict_16m2[spc_code] = spc_count # eg. {'BW':2}																
							# sum up
							c_spc_count[plotname] = [spc_dict_8m2, spc_dict_16m2] # eg. {'P1':[{'BW':2, 'SW':1}, {'BW':2}] }
						
						# if the total tree count of this plot is still zero, this site is unoccupied.
						# this can happen if Unoccupied = No, but the number of total trees in the plot is zero.
						if p_num_trees == 0: 
							site_occ -= 1
							c_site_occ_raw[plotname] = 0
							site_occ_reason[plotname] = 'Unspecified'
							c_spc_count[plotname] = None


				self.logger.debug("c_spc_count = %s"%(c_spc_count))
				# eg. TIM-Gil01 201 c_spc_count = {'P1': [{'BF': 1}, {'PJ': 2}], 'P2': None, 'P3': [{'PT': 2, 'LA': 1, 'CE': 2}, {'PJ': 2, 'PW': 1}],
				# 	'P4': [{'PR': 2, 'SB': 1}, {'PJ': 2}], 'P5': None, 'P6': [{'PW': 2, 'PR': 1}, {}], 'P7': [{}, {}], 'P8': [{'BF': 2}, {'PJ': 3}]}
				self.logger.debug("c_num_trees = %s"%(c_num_trees))



				# Calculating effective density (ED)
				# for clearcut sites, the survey area is 8m2
				# for shelterwood sites, the survey area is both 8m2 and 16m2
				# the ED is calculated for 8m2 and 16m2, then the numbers added together for final ED.
				# for example, a cluster where 14 trees are found in 8m2, and 6 trees in 16m2,
				# ED = '14trees'x 10000/('8m2'x'8plots') + '6trees'x 10000/('16m2'x'8plots') = 2187.5 + 468.75 = 2656.25
				tree_count_8m2 = 0
				tree_count_16m2 = 0
				for plot_num, spc_info in c_spc_count.items():
					if spc_info != None:
						for spc8m2, count8m2 in spc_info[0].items():
							tree_count_8m2 += count8m2
						for spc16m2, count16m2 in spc_info[1].items():
							tree_count_16m2 += count16m2
				# number of trees for each cluster shouldn't exceed the limit
				# if we consider upper limit of 0.5 tree per m2, 64x0.5= 32 max trees for CC, and 128x0.5=64 max trees for SH
				tree_count_max_8m2 = 8*8*self.max_num_of_t_per_sqm
				tree_count_max_16m2 = 16*8*self.max_num_of_t_per_sqm
				if tree_count_8m2 > tree_count_max_8m2: tree_count_8m2 = tree_count_max_8m2
				if tree_count_16m2 > tree_count_max_16m2: tree_count_16m2 = tree_count_max_16m2
				# Calculate Effective Density
				c_eff_dens = (tree_count_8m2*10000/(8*8)) + (tree_count_16m2*10000/(16*8))
				self.logger.debug("c_eff_dens = %s"%(c_eff_dens))

				# Site Occupancy
				site_occ = float(site_occ)/self.num_of_plots # this will give you the site occupancy value between 0 and 1. eg. site_occ = 0.875, 

				# assemble the collected information to the record dictionary.
				record[self.c_comments] = comments_dict # eg. {'cluster': '7 staff', 'P1': 'all residual trees', ... }
				record[self.c_photos] = photos_dict
				record[self.c_site_occ_raw] = c_site_occ_raw
				record[self.c_site_occ] = site_occ
				record[self.c_site_occ_reason] = site_occ_reason # eg. {'P1':'', 'P2': 'Road', ...}
				record[self.c_spc_count] = c_spc_count # eg. {'P1': [{'BF': 1}, {'PJ': 2}], 'P2': None, ...}
				record[self.c_num_trees] = c_num_trees # eg. 15
				record[self.c_eff_dens] = c_eff_dens
				record[self.c_invalid_spc_code] = invalid_spc_codes # eg. [[],['XY'],[],[],...]

				self.logger.debug("Site Occ = %s"%site_occ)
				self.logger.debug("photos_dict = %s"%photos_dict)


				# we've gathered all the information we need from the cluster_survey table, but we need to summarize them.
				# summarizing c_spc_count into the following formats:
				spc_comp = {spc:0 for spc in self.spc_to_check}  # {spcname:count} eg. {'PB': 0, 'PT': 0, 'PO': 0 ...}
				spc_comp_grp = {spcgrp:0 for spcgrp in self.spc_group_dict.keys()} # {spcgrpname:count} eg. {'PO': 0,...}
				spc_comp_tree_count = 0 

				# loop through c_spc_count
				for plot_name, spc_info in c_spc_count.items():
					if spc_info != None:
						# spc_info is a list with two dictionaries: eg. [{'PT': 2, 'LA': 1, 'CE': 2}, {'PJ': 2, 'PW': 1}]
						for spc_count in spc_info:
							for spc_name, count in spc_count.items(): # eg. spc_name = 'PT' and count = 2
								spc_comp[spc_name] += count
								spc_comp_tree_count += count
								# populate the spc_comp_grp
								for grp, spcs_lst in self.spc_group_dict.items():
									if spc_name in spcs_lst:
										spc_comp_grp[grp] += count
										break # no need to loop through the rest of the spc_group_dict

				# spc_comp_tree_count should match c_num_trees we derived above. double checking it here
				if spc_comp_tree_count != c_num_trees:
					self.logger.info("!!!! ProjID: %s clus %s. Total number of trees error: spc_comp_tree_count=%s, c_num_trees = %s"%(cluster[self.proj_id], 
						cluster['ClusterNumber'], spc_comp_tree_count, c_num_trees))

				# throw out species where its count = 0
				spc_comp = {k:v for k,v in spc_comp.items() if v > 0} # eg. {'PB': 2, 'PT': 1, 'PO': 3 ...}
				spc_comp_grp = {k:v for k,v in spc_comp_grp.items() if v > 0} # eg. {'PO': 6,...}

				# calculate percentage
				spc_comp_perc = {k:round(float(v)*100/spc_comp_tree_count,1) for k,v in spc_comp.items()}
				spc_comp_grp_perc = {k:round(float(v)*100/spc_comp_tree_count,1) for k,v in spc_comp_grp.items()}

				self.logger.debug("spc_comp: %s"%spc_comp) # eg. {'BW': 2, 'PB': 1, 'PT': 13}
				self.logger.debug("spc_comp_grp: %s"%spc_comp_grp) # eg.{'BW': 2, 'PO': 14}
				self.logger.debug("spc_comp_perc: %s"%spc_comp_perc) # eg. {'BW': 12.5, 'PB': 6.2, 'PT': 81.2}
				self.logger.debug("spc_comp_grp_perc: %s"%spc_comp_grp_perc) # eg. {'BW': 12.5, 'PO': 87.5}

				# assemble the collected information to the record dictionary.
				record[self.c_spc_comp] = spc_comp
				record[self.c_spc_comp_grp] = spc_comp_grp
				record[self.c_spc_comp_perc] = spc_comp_perc
				record[self.c_spc_comp_grp_perc] = spc_comp_grp_perc


				# ecosite values:
				ecosite = cluster['MoistureEcosite'] # moisture and nutrient eg. 'wet'
				eco_nutri = cluster['NutrientEcosite01'] # eg. Poor, Very Poor, Rich...
				eco_comment = cluster['CommentsEcosite'].replace("'","") # eg. 'this is a landing site'

				self.logger.debug("c_ecosite: %s"%ecosite)
				self.logger.debug("c_eco_comment: %s"%eco_comment)
				self.logger.debug("c_eco_nutri: %s"%eco_nutri)

				record[self.c_ecosite] = ecosite
				record[self.c_eco_comment] = eco_comment
				record[self.c_eco_nutri] = eco_nutri


				# all these records components are assembled and appended as a new record in the cluster summary table.
				# self.logger.info("cluster summary: %s"%record)
				self.clus_summary_dict_lst.append(record)

# example cluster summary: {'cluster_uid': 398, 'cluster_number': '21', 'proj_id': 'WAW-NAG-395', 'creation_date': '2021-10-15', 
# 'silvsys': 'CC', 'spc_count': {'P1': [{'SB': 1}, {}], 'P2': [{'SB': 2}, {}], 'P3': None, 'P4': [{'SB': 1}, {}], 'P5': [{'SB': 1}, {}], 'P6': [{'SB': 2}, {}], 'P7': None, 'P8': [{'BF': 1}, {}]}, 
# 'total_num_trees': 8, 'effective_density': 1250.0, 'invalid_spc_codes': [], 'site_occ_data': {'P1': 1, 'P2': 1, 'P3': 0, 'P4': 1, 'P5': 1, 'P6': 1, 'P7': 0, 'P8': 1}, 'site_occ': 0.75, 
# 'site_occ_reason': {'P1': '', 'P2': '', 'P3': 'Treed', 'P4': '', 'P5': '', 'P6': '', 'P7': 'Treed', 'P8': ''}, 'cluster_comments': {'P1': '', 'P2': '', 'P3': 'Pj over 10cm', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''}, 
# 'photos': {'cluster': 'images/connectspatial/795c9edd-d4db-4c46-bb8c-6a9d99d06050.jpg', 'P1': '', 'P2': '', 'P3': '', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''}, 'spc_comp': {'BF': 1, 'SB': 7}, 
# 'spc_comp_grp': {'BF': 1, 'SX': 7}, 'spc_comp_perc': {'BF': 12.5, 'SB': 87.5}, 'spc_comp_grp_perc': {'BF': 12.5, 'SX': 87.5}, 'ecosite_moisture': 'fresh', 'ecosite_nutrient': 'Unknown', 
# 'ecosite_comment': '', 'lat': '48.78838752', 'lon': '-84.58135279'}


	def photo_alternate_paths(self):
		""" 
		first, rename photos
		second, 3 paths for photos: 
			original = 'images/connectspatial/25aa1a61-367f-4ffa-bda5-e3535df729f4.jpg'
			local sync location = 'C:/Users/kimdan/Government of Ontario/Regeneration Assessment Program - RAP Picture Library/Michaud130_C192_P4.jpg'
			sharepoint = 'https://ontariogov.sharepoint.com/:i:/r/sites/MNRF-ROD-EXT/RAP/RAP%20Picture%20Library/Michaud130_C192_P4.jpg'
		third, copy the photos to the local sync location (if the picture is not already there)
		"""
		self.logger.info("Running photo_alternate_paths method")
		# we will ultimately alter the self.clus_summary_dict_lst. first, we make a copy of it to loop it and change it as we go.
		temp_clus_summary_dict_lst = self.clus_summary_dict_lst.copy()

		
		# loop through the cluster summary records (each record is a dictionary)
		for index, record in enumerate(temp_clus_summary_dict_lst):
			# these two dictionaries will be filled out and added to the clus_summary_dict_lst's record
			c_local_sync_photopath = {}
			c_sharepoint_photopath = {}
			# loop through the photo dictionary i.e. record['photos']			
			for location_taken, url_txt in record[self.c_photos].items():
				# location_taken can be 'cluster' or 'P1'...'P8'
				# url can be 'images/connectspatial/25aa1a61-367f-4ffk.jpg|images/connectspatial/25aa1a61-367f-4ffa.jpg'
				
				c_local_sync_photopath[location_taken] = []
				c_sharepoint_photopath[location_taken] = []	

				# split the url in case there's more than one urls
				urls = url_txt.split('|') # eg. ['images/connectspatial/25aa1a61-367f-4ffk.jpg', 'images/connectspatial/25aa1a61-367f-4ffa.jpg']
				if urls != ['']: # if there's at least one url
					for num, url in enumerate(urls):
						# get the original full-path of the photo
						original_fullpath = os.path.join(self.cfg_dict['INPUT']['inputdatafolderpath'], url) # eg. C:\RAP_2021\data\Regeneration Assessment Program_18-Oct-21_04-55\data\images\connectspatial\25aa1a61-367f-4ffa.jpg
						filename = os.path.split(original_fullpath)[1] #eg. '25aa1a61-367f-4ffk.jpg'
						last4letters = filename[-8:-4] #eg. '4ffk' - last 4 characters of the original filename. This makes the picture tracible to the original and makes the filename unique

						# Rename the photo files
						# proj_id + C + cluster_number + photo_location + last4letters + creation date
						# For example,
						# WAW-NAG-395_C28_cluster_4ffk_2021-10-15.jpg
						# SAU-NSF-4_C16_P1_a23w_2021-10-15.jpg  if there's more than one photo for that location
						new_filename = "%s_C%s_%s"%(record[self.c_proj_id], record[self.c_clus_num], location_taken) # SAU-NSF-4_C16_P1
						new_filename += "_%s"%last4letters # _4ffk
						new_filename += "_%s"%record[self.c_creation_date] # _2021-10-15
						new_filename = new_filename.replace(' ', '') # no blank space should exist in the name
						new_filename += filename[-4:] # .jpg

						new_local_fullpath = os.path.join(self.cfg_dict['OUTPUT']['output_photopath'], new_filename) # eg. 'C:/Users/kimdan/Government of Ontario/Regeneration Assessment Program - RAP Picture Library/SAU-NSF-4_C16_P1_4ffk_2021-10-15.jpg'
						new_sharepoint_fullpath = self.cfg_dict['OUTPUT']['sharepoint_photopath'] + '/' + new_filename # eg. 'https://ontariogov.sharepoint.com/:i:/r/sites/MNRF-ROD-EXT/RAP/RAP%20Picture%20Library/SAU-NSF-4_C16_P1_4ffk_2021-10-15.jpg'
						
						# time to copy over!!
						if not os.path.exists(new_local_fullpath):
							self.logger.info("Copying photo: %s"%new_filename)
							print("Copying photo: %s"%new_filename)
							shutil.copy2(original_fullpath, new_local_fullpath)

						# write the new paths down to the summary dictionary
						c_local_sync_photopath[location_taken].append(new_local_fullpath) 
						c_sharepoint_photopath[location_taken].append(new_sharepoint_fullpath) 

			self.clus_summary_dict_lst[index][self.c_local_sync_photopath] = c_local_sync_photopath
			self.clus_summary_dict_lst[index][self.c_sharepoint_photopath] = c_sharepoint_photopath

		# for i in range(len(self.clus_summary_dict_lst)):
		# 	self.logger.info(str(self.clus_summary_dict_lst[i][self.c_local_sync_photopath]))
		# 	self.logger.info(str(self.clus_summary_dict_lst[i][self.c_sharepoint_photopath]))

# eg. c_local_sync_photopath = {'cluster': ['C:\\Users\\kimdan\\Government of Ontario\\Regeneration Assessment Program - 
# RAP Picture Library\\NOR-HWY11-5_C462_cluster_12fa_2021-09-15.jpg'], 'P1': [], 'P2': ['C:\\Users\\kimdan\\Government of Ontario
# \\Regeneration Assessment Program - RAP Picture Library\\NOR-HWY11-5_C462_P2_4fd7_2021-09-15.jpg'], 'P3': [], 'P4': [], 'P5': 
# ['C:\\Users\\kimdan\\Government of Ontario\\Regeneration Assessment Program - RAP Picture Library\\NOR-HWY11-5_C462_P5_0250_2021
# -09-15.jpg'], 'P6': ['C:\\Users\\kimdan\\Government of Ontario\\Regeneration Assessment Program - RAP Picture Library\\NOR-HWY11-
# 5_C462_P6_f150_2021-09-15.jpg'], 'P7': [], 'P8': []}

# eg. c_sharepoint_photopath = same format as c_local_sync_photopath but with url of sharepoint.


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

		# loop through each record (project) in the shapefile (shapefile but in dictionary form)
		# Note that all keys in prj_shp_in_dict are in upper case
		for prj in self.prj_shp_in_dict:
			# record dictionary will act as a template for this cluster and the values will be filled out as we go.
			record = self.proj_summary_dict.copy()
			proj_id = prj[self.prj_shp_prjid_fieldname] # project id from the shapefile
			self.logger.info('\tWorking on ProjectID: %s'%proj_id)
			p_analysis_comments = [] # comments will be appended here

			# copying information from the shapefile to this summary table:
			record[self.p_proj_id] = proj_id
			record[self.p_num_clus] = prj['NUMCLUSTER']
			record[self.p_silvsys] = prj['SILVSYS']
			record[self.p_area] = prj['AREA_HA']
			record[self.p_plot_size] = 16 if prj['SILVSYS'] =='SH' else 8
			record[self.p_spatial_fmu] = prj['FMU']
			record[self.p_spatial_dist] = prj['DISTRICT']
			record[self.p_lat] = prj['LAT']
			record[self.p_lon] = prj['LON']

			record[self.p_yrdep] = prj['YRDEP']
			record[self.p_depfu] = prj['DEPLETIONF']
			record[self.p_yrorg] = prj['YRORG']
			record[self.p_sgr] = prj['SGR']
			record[self.p_targetfu] = prj['TARGETFU']
			record[self.p_targetspc] = prj['TARGETSPC']
			record[self.p_targetso] = prj['TARGETSO']

			record[self.p_sfl_as_yr] = prj['SFL_AS_YR']
			record[self.p_sfl_as_method] = prj['SFL_ASMETH']
			record[self.p_sfl_spcomp] = prj['SFL_SPCOMP']
			record[self.p_sfl_so] = prj['SFL_SO']
			record[self.p_sfl_fu] = prj['SFL_FU']
			record[self.p_sfl_effden] = prj['SFL_EFFDEN']


			# grap and summarize cluster data (clus_summary_dict_lst) into project summary
			cluster_data_of_this_proj = []
			cluster_num_lst = []
			for clus_summary in self.clus_summary_dict_lst:
				if clus_summary['proj_id'] == proj_id:
					cluster_data_of_this_proj.append(clus_summary)
					cluster_num_lst.append(clus_summary['cluster_number'])
			cluster_num_lst.sort()

			# check for duplicate cluster number
			duplicate_clus = set([clus_num for clus_num in cluster_num_lst if cluster_num_lst.count(clus_num) > 1])
			if len(duplicate_clus) > 0:
				duplicate_clus = list(duplicate_clus)
				duplicate_clus_str = ''
				for clus in duplicate_clus:
					duplicate_clus_str += clus + ', '
				duplicate_clus_str = duplicate_clus_str[:-2]
				self.logger.info("!!!! Duplicate cluster found: %s"%duplicate_clus_str)
				p_analysis_comments.append("Duplicate cluster found: %s"%duplicate_clus_str)

			# Number of clusters surveyed as far
			num_clus_surveyed = len(cluster_num_lst)
			self.logger.info("\t\tSurveyed Cluster Count: %s of %s"%(num_clus_surveyed,prj['NUMCLUSTER']))
			is_survey_complete = False if num_clus_surveyed < int(prj['NUMCLUSTER']) else True
			record[self.p_num_clus_surv] = num_clus_surveyed
			record[self.p_lst_of_clus] = cluster_num_lst		
			record[self.p_is_complete] = is_survey_complete


			# Assessment start and last assessment date
			clus_survey_dates = [] # eg. ['2021-09-14', '2021-09-14', '2021-10-04',...]
			for cluster in cluster_data_of_this_proj:
				clus_survey_dates.append(cluster[self.c_creation_date])
			if len(clus_survey_dates) < 1:
				assess_start_date = ''
				assess_last_date = ''
			else:
				clus_survey_dates.sort()
				assess_start_date = clus_survey_dates[0]
				assess_last_date = clus_survey_dates[-1]
			record[self.p_assess_start_date] = assess_start_date
			record[self.p_assess_last_date] = assess_last_date


			# Assessors (surveyors), Surveyor's FMU, Surveyor's District
			# These information is available not in the cluster summary but in the raw data (cc_cluster_in_dict)
			cluster_raw_data = self.sh_cluster_in_dict if prj['SILVSYS'] == 'SH' else self.cc_cluster_in_dict
			assessors_lst = []
			surveyors_fmu_lst = []
			surveyors_dist_lst = []
			for cluster in cluster_raw_data:
				if cluster[self.fin_proj_id] == proj_id:
					assessors_lst.append(cluster['Surveyors'])
					surveyors_fmu_lst.append(cluster['ForestManagementUnit'])
					surveyors_dist_lst.append(cluster['DistrictName'])
			assessors = [i for i in set(assessors_lst) if len(i)>0]
			surveyors_fmu = [i for i in set(surveyors_fmu_lst) if len(i)>0]
			surveyors_dist = [i for i in set(surveyors_dist_lst) if len(i)>0]
			record[self.p_assessors] = assessors # eg. ['Mitchell Sissing', 'Group ']
			record[self.p_fmu] = surveyors_fmu
			record[self.p_dist] = surveyors_dist # eg. ['North Bay']

			# comments summary
			comments = {} # combination of all comments
			for cluster in cluster_data_of_this_proj:
				comments[cluster['cluster_number']] = cluster[self.c_comments]
			record[self.p_comments] = comments # eg. {'456': {'cluster': '', 'ecosite': '', 'P1': '',...}

			# Effective density data eg. {'109': 1225, '108': 1375,...}
			effective_density_data = {}
			for cluster in cluster_data_of_this_proj:
				effective_density_data[cluster['cluster_number']] = cluster[self.c_eff_dens]
			# Effective density # eg. {'mean': 1979.1667, 'stdv': 1271.9428, 'ci': 1334.8221, 'upper_ci': 3313.9888, 'lower_ci': 644.3446, 'n': 6, 'confidence': 0.95}
			effective_density = mymath.mean_std_ci(effective_density_data) 
			record[self.p_effect_dens_data] = effective_density_data
			record[self.p_effect_dens] = effective_density

			# number of clusters where at least 1 plot is occupied with trees # this should be the n for species calculation
			lst_of_occupied_clus = []
			for cluster in cluster_data_of_this_proj:
				if cluster[self.c_site_occ] > 0:
					lst_of_occupied_clus.append(cluster['cluster_number'])
			lst_of_occupied_clus = list(set(lst_of_occupied_clus)) # removing duplicate clusters (there shouldn't be duplicates)
			num_cl_occupied = len(lst_of_occupied_clus)
			record[self.p_num_cl_occupied] = num_cl_occupied

			# site occupancy
			so_data = {} # eg. {'109': 0.875, '108': 1...}
			so_reason = {} # eg. {'109': {'P1': 'Treed', 'P2': ''...}}, '108': {'P1': 'Shrubs', 'P2': '', }}
			for cluster in cluster_data_of_this_proj:
				so_data[cluster['cluster_number']]=cluster[self.c_site_occ]
				so_reason[cluster['cluster_number']]=cluster[self.c_site_occ_reason]
			so = mymath.mean_std_ci(so_data)
			record[self.p_so_data] = so_data
			record[self.p_so] = so
			record[self.p_so_reason] = so_reason


			# SPECIES ANALYSIS: 'species_found', 'species_grps_found', 'species_data_percent', 'species_grp_data_percent', 'spcomp', spcomp_grp'
			spc_dict = {} # eg. {'109': {'BW': 30.0, 'SW': 70.0}, '108': {'BF': 18.2, 'LA': 9.1, 'SW': 72.7},...}
			spc_grp_dict = {}
			for cluster in cluster_data_of_this_proj:
				if cluster[self.c_site_occ] > 0:
					spc_dict[cluster['cluster_number']] = cluster[self.c_spc_comp_perc]
					spc_grp_dict[cluster['cluster_number']] = cluster[self.c_spc_comp_grp_perc]
			# calculate spc_found and spc_grp_found
			spc_found = [] # eg.['CB', 'BN', 'SW', 'LA', 'BW', 'BF']
			spc_grp_found = [] # eg. ['CB', 'BN', 'LA', 'BW', 'SX', 'BF']
			for v in spc_dict.values():
				for i in v.keys():
					spc_found.append(i)
			for v in spc_grp_dict.values():
				for i in v.keys():
					spc_grp_found.append(i)					
			spc_found = list(set(spc_found))
			spc_grp_found = list(set(spc_grp_found))
			# calculate spc_data and spc_grp_data (n = len(lst_of_occupied_clus))
			clusters_dict = {clus_num:0 for clus_num in lst_of_occupied_clus} # eg. {'109':0, '103':0, '104':0,...}
			spc_data = {spc:clusters_dict.copy() for spc in spc_found} # eg. {'BF': {'109':0, '103':0}, 'BW': {'109':0, '103':0}, ...}
			spc_grp_data = {spc:clusters_dict.copy() for spc in spc_grp_found}
			# calculate spcomp and spcomp_grp
			for clus_num, spc_rec in spc_dict.items():
				for spc, perc in spc_rec.items():
					spc_data[spc][clus_num] = perc
			for clus_num, spc_rec in spc_grp_dict.items():
				for spc, perc in spc_rec.items():
					spc_grp_data[spc][clus_num] = perc
			# calculate p_spc and p_spc_grp (mean, stdev, etc.)
			spc = {spc: mymath.mean_std_ci(data) for spc, data in spc_data.items()}
			spc_grp = {spc: mymath.mean_std_ci(data) for spc, data in spc_grp_data.items()}
			record[self.p_spc_found] = spc_found # ['CE', 'BF', 'PO', 'PB', 'BW', 'PT']
			record[self.p_spc_grp_found] = spc_grp_found # ['CE', 'BW', 'BF', 'PO']
			record[self.p_spc_data] = spc_data # {'CE': {'25': 0, '3': 25.0, '20': 0, ...}, 'BF': {'25': 0, '3': 25.0, '20': 0,...}}
			record[self.p_spc_grp_data] = spc_grp_data 
			record[self.p_spc] = spc # {'CE': {'mean': 1.7857, 'stdv': 6.6815, 'ci': 3.8578, ...}, 'BF': {'mean': 1.7857, 'stdv': 6.6815, 'ci'...}}
			record[self.p_spc_grp] = spc_grp

			# ecosite
			ecosite_data = {} # eg. {'109':['moist','rich in nutrient','some comment'], '103':['dry','',''],...}
			for cluster in cluster_data_of_this_proj:
				ecosite_data[cluster['cluster_number']] = [cluster[self.c_ecosite],cluster[self.c_eco_nutri],cluster[self.c_eco_comment].replace("'","")]
			if len(ecosite_data) > 0:
				moist = list(set([eco[0] for eco in ecosite_data.values()]))
				eco_moisture = {i:0 for i in moist} #eg. {'moist': 0, 'dry':0, ...}
				eco_count = 0
				for eco in ecosite_data.values():
					eco_moisture[eco[0]] += 1
					eco_count += 1
				# turn the count into percent
				eco_moisture = {k:round(float(v)*100/eco_count, 1) for k,v in eco_moisture.items()}
			else:
				eco_moisture = {} 
			record[self.p_ecosite_data] = ecosite_data # {'356': ['fresh', 'Moderately Rich', ''], '357': ['fresh', 'Moderately Rich', ''],...}
			record[self.p_eco_moisture] = eco_moisture # {'moist': 3.3, 'wet': 3.3, 'fresh': 93.3}

			# add analysis comments and warnings
			record[self.p_analysis_comments] = p_analysis_comments

			# finally, append the record to the table
			self.proj_summary_dict_lst.append(record)



	def proj_summary_to_sqlite(self):
		""" Writing the cluster summary dictionary list to a brand new table in the sqlite database.
		"""
		common_functions.dict_lst_to_sqlite(self.proj_summary_dict_lst, self.db_filepath, self.proj_summary_tblname, self.logger)



	def create_plot_table(self):
		"""
		go through self.clus_summary_dict_lst again and create two plot summary tables on the sqlite database. (one for CC and one for SH)
		This table would be closest thing to the raw data collected.
		"""
		# first we need a list of all species codes found in the raw data
		# this list will be used to create attribute names of the plot_summary table.
		all_spc_codes_from_raw_data = []
		for record in self.clus_summary_dict_lst:
			for data in record[self.c_spc_count].values(): # eg. data =  [{'PL': 1, 'MR': 1}, {}] or just None
				if data != None:
					for item in data: # eg. item = {'PL': 1, 'MR': 1}
						for spc_code in item.keys():
							if len(spc_code) > 0 and spc_code not in all_spc_codes_from_raw_data:
								all_spc_codes_from_raw_data.append(spc_code)
		all_spc_codes_from_raw_data.sort()

		# create a record template
		plot_summary_dict = {}

		# loop through the clusters and populate each records
		for clus_record in self.clus_summary_dict_lst:
			silvsys = clus_record[self.c_silvsys] # 'CC' or 'SH'
			# loop through the number of plots we have
			for i in range(self.num_of_plots):
				plot_record = plot_summary_dict.copy()
				plotnum = str(i+1)
				plotname = 'P' + plotnum

				plot_record['proj_id'] = clus_record[self.c_proj_id]
				plot_record['cluster_num'] = clus_record[self.c_clus_num]
				plot_record['plot_num'] = plotnum
				plot_record['site_occupied'] = clus_record[self.c_site_occ_raw][plotname]
				plot_record['reason_for_unoccupancy'] = clus_record[self.c_site_occ_reason][plotname]

				# get tree counts for each species for each plot
				# note that for each species, we need 2 counts - one for 8sqm and one for 16sqm
				if silvsys == 'SH':
					plotsizes = ['8sqm', '16sqm']
					for spc_code in all_spc_codes_from_raw_data:
						for index, plotsize in enumerate(plotsizes):
							spc_count = clus_record[self.c_spc_count] # eg {'P1': [{'PL': 1, 'MR': 1}, {}], 'P2': None,...}
							try:
								spc_count = spc_count[plotname][index][spc_code]
							except (KeyError, TypeError):
								spc_count = 0
							plot_record[spc_code+'_'+plotsize] = spc_count
					self.plot_summary_dict_lst_sh.append(plot_record)
				else:
					for spc_code in all_spc_codes_from_raw_data:
						spc_count = clus_record[self.c_spc_count] # eg {'P1': [{'PL': 1, 'MR': 1}, {}], 'P2': None,...}
						try:
							spc_count = spc_count[plotname][0][spc_code]
						except (KeyError, TypeError):
							spc_count = 0
						plot_record['_'+spc_code] = spc_count

					self.plot_summary_dict_lst_cc.append(plot_record)


		# create the table on the sqlite database
		self.plotcount_cc_sh = {'CC': len(self.plot_summary_dict_lst_cc), 'SH':len(self.plot_summary_dict_lst_sh)}
		if self.plotcount_cc_sh['CC'] > 0:
			common_functions.dict_lst_to_sqlite(self.plot_summary_dict_lst_cc, self.db_filepath, self.plot_summary_tblname + '_cc', self.logger)
		else:
			self.logger.info("!!!! There's no CC clusters/plots. Cannot create plot summary table for CC")
		if self.plotcount_cc_sh['SH'] > 0:	
			common_functions.dict_lst_to_sqlite(self.plot_summary_dict_lst_sh, self.db_filepath, self.plot_summary_tblname + '_sh', self.logger)
		else:
			self.logger.info("!!!! There's no SH clusters/plots. Cannot create plot summary table for SH")





	def create_z_tables(self):
		""" a table will be created for each project. table name example: z_NOR-PAPINEAU-2
		Each of these tables will carry processed data in a easily readable format.
		This will make it easy to print out on browsers and etc.
		Ingredients:
			self.proj_summary_dict_lst
			self.clus_summary_dict_lst
		"""
		
		active_projs = [record['proj_id'] for record in self.proj_summary_dict_lst if int(record[self.p_num_clus_surv]) > 0]

		# create tables
		for proj in active_projs:
			proj_sum_dict = [record for record in self.proj_summary_dict_lst if record['proj_id'] == proj][0] # this returns a single record in Project_summary table in a dictionary format
			# clus_sum_dict = [record for record in self.clus_summary_dict_lst if record['proj_id'] == proj]
			lst_of_clus = sorted(list(set(proj_sum_dict[self.p_lst_of_clus]))) # eg ['179', '183', '184', '189', '190', '901']
			num_of_rows = len(lst_of_clus)
			
			# deciding the attributes for this new table
			attr = ['Cluster_Num', 'Site_Occ', 'Ef_Density', 'Moisture', 'Silvsys']
			spc_list = sorted(['_'+ spcname for spcname in proj_sum_dict[self.p_spc_found]]) # ['_AB', '_CE', '_OR', '_PT', '_PW', '_SB', '_SW']
			attr += spc_list

			# making an empty list of dictionaries which will later turn into a table
			rec_template = {attribute:'' for attribute in attr} #eg {'Cluster Num': '', 'Site Occ': '', 'Ef Density': '', 'Moisture': '', 'AB': '',...}
			table = [] # will be filled with filled out rec_templates

			# fill out the table
			for clus in lst_of_clus:
				rec = rec_template.copy()
				rec['Cluster_Num'] = clus
				rec['Site_Occ'] = proj_sum_dict[self.p_so_data][clus] # 0.75
				rec['Ef_Density'] = proj_sum_dict[self.p_effect_dens_data][clus] # 1446
				rec['Moisture'] = proj_sum_dict[self.p_ecosite_data][clus][0] # 'moist'
				rec['Silvsys'] = proj_sum_dict[self.p_silvsys] # 'CC'
				for spc in spc_list:
					try:
						rec[spc] = proj_sum_dict[self.p_spc_data][spc[1:]][clus] # percent of that species in this cluster
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
		self.photo_alternate_paths()
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
	

