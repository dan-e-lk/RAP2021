version = '2021.09'


import sys, os, pprint, traceback
print(sys.version)

# import custom modules
from modules import common_functions, csv2sqlite, determine_project_id, analysis, log, shp2sqlite, to_csv, to_browsers


def sem(configfilepath, initial_msg, custom_datapath = None, ignore_testdata = True):
	"""configfile carries most of the static variables. configfile is typically located in the same folder as this script: SEM.cfg
	initial_msg is used when another program such as TDT is run before this script run. The message will be carried on to the log file.
	custom_datapath is used when TDT did is run right before this tool. custom_datapath will replace config's CSV.folderpath variable.
	For example, if TDT downloads new set of data at C:\raw_data\RAP_project_2020-07-13_4\data folder, this should be entered as the custom_datapath
	"""
	timenow = common_functions.datetime_readable() #eg. Apr 21, 2020. 02:09 PM

	# grab the config file
	config_file = configfilepath
	cfg_dict = common_functions.cfg_to_dict(config_file)

	# if custom datapath is available, use that instead of the path given in the cfg file.
	if custom_datapath != None:
		if os.path.isdir(custom_datapath):
			cfg_dict['CSV']['csvfolderpath'] = custom_datapath

	# start logging
	logfile = os.path.join(os.path.split(os.path.abspath(__file__))[0], 'log_sem.txt')
	debug = True if cfg_dict['LOG']['debug'].upper() == 'TRUE' else False

	logger = log.logger(logfile, debug)
	logger.info('### ## #  Launching SEM program  # ## ###')
	logger.info('Time: %s'%timenow)
	logger.info('version %s'%version)
	logger.info(initial_msg)
	logger.info('Config file used: %s'%configfilepath)
	logger.info('Survey Data being used: %s'%cfg_dict['CSV']['csvfolderpath'])
	logger.info('All variables from the config file:\n' + pprint.pformat(cfg_dict))


	try:

		## insert "check config file" module here ##
		# check the path of every path variables in the config file


		# Other things that needs checking:



		## grabbing (and checking) spcies group from SpeciesGroup.csv
		spc_to_check, spc_group_dict = common_functions.open_spc_group_csv(cfg_dict['SPC']['csv'])
		logger.info("spc_to_check = %s"%spc_to_check)
		logger.info("spc_group_dict = %s"%spc_group_dict)


		# csv2sqlite
		# creating sqlite database from the csv files
		c2s = csv2sqlite.Csv2sqlite(cfg_dict['CSV']['csvfolderpath'],cfg_dict['CSV']['dbfolderpath'],cfg_dict['SQLITE']['unique_id_fieldname'],logger, ignore_testdata)
		db_filepath = c2s.db_fullpath_new
		tablenames_n_rec_count = c2s.tablenames_n_rec_count

		logger.debug("Checkpoint after csv2sqlite:\ndb_filepath = %s\ntablenames_n_rec_count = %s"%(db_filepath,tablenames_n_rec_count))



		# shp2sqlite
		# creating sqlite table from the shp file (project boundaries and info)
		s2s = shp2sqlite.Shp2sqlite(cfg_dict, db_filepath, tablenames_n_rec_count, logger)
		s2s.run_all()
		tablenames_n_rec_count = s2s.tablenames_n_rec_count
		# logger.debug('******** %s'%tablenames_n_rec_count)



		# determine_project_id
		dp = determine_project_id.Determine_project_id(cfg_dict, db_filepath, tablenames_n_rec_count, logger)
		dp.run_all()
		# return some variables that may be used later on in the script
		tablenames_n_rec_count, uniq_id_to_proj_id, cluster_tbl_name, project_tbl_name, dp_summary_dict = dp.return_updated_variables()



		# analysis
		# Species and Site Occupancy analysis begins here:
		ana = analysis.Run_analysis(cfg_dict, db_filepath, cluster_tbl_name, project_tbl_name, spc_to_check, spc_group_dict, logger)
		ana.run_all()
		# we will need the attribute names of cluster summary and proj summary tables:
		clus_summary_attr = ana.clus_summary_attr # eg. {'c_clus_uid': 'cluster_uid', 'c_clus_num': 'cluster_number', 'c_proj_id': 'proj_id',...}
		proj_summary_attr = ana.proj_summary_attr


		# to_csv
		tocsv = to_csv.To_csv(cfg_dict, db_filepath, clus_summary_attr, proj_summary_attr, logger)
		tocsv.run_all()

		# to_browsers
		to_b = to_browsers.To_browsers(cfg_dict, db_filepath, logger)
		to_b.run_all()


		# end of sem program
		logger.info('SEM program completed\n\n\n')

	except:
		# if any error encountered, log it.
		var = traceback.format_exc()
		print(var)
		logger.info(var)





if __name__ == '__main__':
	configfile = 'RAP.cfg'
	initial_msg = "Stand-alone SEM.py run - TDT tool did not run!"
	sem(configfile, initial_msg)