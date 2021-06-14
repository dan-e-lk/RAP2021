# this module comes after analysis.py module.
# the purpose of this module is to write sqlite database to csv files where possible and needed.


import sqlite3, csv, os

# importing custom modules
if __name__ == '__main__':
	import common_functions
else:
	from modules import common_functions



class To_csv:
	def __init__(self, cfg_dict, db_filepath, clus_summary_attr, proj_summary_attr, logger):
		self.db_filepath = db_filepath
		self.logger = logger
		self.clus_summary_attr = clus_summary_attr # dictionary of variable: attribute names. they were defined in analysis.py's define_attr_names() method.
		self.proj_summary_attr = proj_summary_attr
		self.clus_summary_tblname = cfg_dict['SQLITE']['clus_summary_tblname']
		self.proj_summary_tblname = cfg_dict['SQLITE']['proj_summary_tblname']
		self.plot_summary_tblname = cfg_dict['SQLITE']['plot_summary_tblname']
		self.projects_shp = cfg_dict['SHP']['shp2sqlite_tablename']
		self.output_csv_folderpath = cfg_dict['CSV']['output_csv_folderpath']
		self.calc_max = int(cfg_dict['CALC']['num_of_trees_4_spcomp'])

		self.logger.info("\n")
		self.logger.info("--> Running To_csv module")


	def tbl_2_dict(self):
		"""Turns sqlite tables into list of dictionaries"""
		self.plot_summary_dict = common_functions.sqlite_2_dict(self.db_filepath, self.plot_summary_tblname)
		self.clus_summary_dict = common_functions.sqlite_2_dict(self.db_filepath, self.clus_summary_tblname)
		self.proj_summary_dict = common_functions.sqlite_2_dict(self.db_filepath, self.proj_summary_tblname)

		# get project ids that actually has any data collected
		self.active_projs = [record['proj_id'] for record in self.proj_summary_dict if int(record['num_clusters_surveyed']) > 0]
		self.logger.info("Active Projects: %s"%self.active_projs)


	def plot_to_csv(self):
		""" converts self.plot_summary_dict into a csv file"""
		self.logger.info("Running plot_to_csv method")

		# find all attribute names
		attr = self.plot_summary_dict[0].keys()

		outputcsv = os.path.join(self.output_csv_folderpath,'_all_plots.csv')
		try:
			with open(outputcsv, 'w') as f:
				writer = csv.DictWriter(f, fieldnames=attr, lineterminator='\n')
				writer.writeheader()
				writer.writerows(self.plot_summary_dict)
		except PermissionError:
			self.logger.info("!!!!! Error - could not create %s. Check if the file is being used."%outputcsv)


	def clus_to_csv(self):
		""" write out the calculations done for each projects.
			the output will be csv files saved as "projectname_calc.csv"
		"""
		self.logger.info("Running clus_to_csv")
		timenow = common_functions.datetime_readable() #eg. Apr 21, 2020. 02:09 PM

		# grabbing the attribute names of clus_summary and proj_summary tables in sqlite database.
		# note that these were defined in analysis.py
		num_tree_attr = self.clus_summary_attr['c_num_trees'] #'total_num_trees'
		eff_dens_attr = self.clus_summary_attr['c_eff_dens'] #'effective_density'
		so_data_attr = self.clus_summary_attr['c_site_occ'] #'site occupancy data'
		so_reason = self.clus_summary_attr['c_site_occ_reason'] #'site occupancy unoccupied reasons'
		so_attr = self.proj_summary_attr['p_so'] #'site occupancy calculation'
		residual_data_attr = self.proj_summary_attr['p_residual_data'] #'residual data'
		residual_count_attr = self.proj_summary_attr['p_residual_count'] #'residual count'
		residual_percent_attr = self.proj_summary_attr['p_residual_percent'] #'residual count'
		residual_BA_attr = self.proj_summary_attr['p_residual_BA'] #'residual count'
		lst_of_clus_attr = self.proj_summary_attr['p_lst_of_clus'] #'list of clusters'
		ecosite_data_attr = self.proj_summary_attr['p_ecosite_data'] #'ecosite data'
		ecosite_moisture_attr = self.proj_summary_attr['p_eco_moisture'] #'ecosite moisture'
		analysis_comments_attr = self.proj_summary_attr['p_analysis_comments'] # 'analysis comments' that contains any warnings/errors found during analysis
		spc_data_attr = self.proj_summary_attr['p_spc_data']
		spc_grp_data_attr = self.proj_summary_attr['p_spc_grp_data']
		spcomp_attr = self.proj_summary_attr['p_spc']
		spcomp_grp_attr = self.proj_summary_attr['p_spc_grp']


		for p in self.active_projs:
			csvfilename = os.path.join(self.output_csv_folderpath, p + '_calc.csv')
			data = [record for record in self.clus_summary_dict if record['proj_id']==p] # a list of dictionaries equivalent of the cluster_summary table's records of this project id
			proj_summary_record = [record for record in self.proj_summary_dict if record['proj_id']==p]
			proj_summary_record = proj_summary_record[0] # a dictionary equivalent of the project_summary table's one record
			lst_of_clus = eval(proj_summary_record[lst_of_clus_attr]) # ['707','701', '701', '702', '703', '704', '705', '706'] i.e. not sorted
			lst_of_clus = common_functions.sort_integers(lst_of_clus) # ['701', '701', '702', '703', '704', '705', '706', '707'] i.e. sorted
			warnings = eval(proj_summary_record[analysis_comments_attr]) # ['Duplicate clusters found: ['701']']
			if len(set(lst_of_clus)) < 2: warnings.append('Less than 2 distinct clusters collected - unable to run statistics on just one sample.')
			lat = proj_summary_record['lat']
			lon = proj_summary_record['lon']
			area = proj_summary_record['area_ha']
			sfl_spcomp = proj_summary_record['sfl_spcomp']
			sfl_so = proj_summary_record['sfl_so']
			sfl_fu = proj_summary_record['sfl_fu']
			sfl_effden = proj_summary_record['sfl_effden']
			sfl_as_yr = proj_summary_record['sfl_as_yr']

			# start writing csv file
			self.logger.debug("writing csv file: %s"%csvfilename)
			try:
				with open(csvfilename,'w') as f:
					writer = csv.writer(f, lineterminator='\n')
					writer.writerow(['Calculations for project id = %s'%p])
					writer.writerow(['Last Updated: %s'%timenow])
					writer.writerow(['Project location (lat, long): %s, %s'%(lat,lon)])
					writer.writerow(['Project Area: %sha'%area])
					writer.writerow(['SFL SPCOMP: %s'%sfl_spcomp])
					writer.writerow(['SFL Site Occupancy: %s'%sfl_so])
					writer.writerow(['SFL Forest Unit: %s'%sfl_fu])
					writer.writerow(['SFL Effective Density: %s'%sfl_effden])
					writer.writerow(['SFL Assessment Year: %s'%sfl_as_yr])
					writer.writerow('')
					if len(warnings) > 0:
						writer.writerow(['<WARNINGS>'])
						for warning in warnings:
							writer.writerow([warning])
						writer.writerow('')

			# SPCOMP data and result
				spc_data = eval(proj_summary_record[spc_data_attr]) # {'SW': {'179': 43.8, '183': 85.7, '190': 80.0, '189': 70.0, '184': 72.7}, 'BF': {'179': 0, '183': 7.1, '190': 10.0, '189': 0, '184': 18.2},...}
				spcomp = eval(proj_summary_record[spcomp_attr]) # {'SW': {'mean': 70.44, 'stdv': 16.1187, 'ci': 20.014, 'upper_ci': 90.454, 'lower_ci': 50.426, 'n': 5, 'confidence': 0.95}, 'BF':...}
				if len(spc_data) > 0:
					spc_list = list(spc_data.keys())
					attr = ['cluster_number'] + spc_list
					with open(csvfilename,'a') as f:
						writer = csv.writer(f, lineterminator='\n')
						writer.writerow(['<SPCOMP>'])
						writer.writerow(attr)
						for clus in lst_of_clus:
							row = [clus]
							for spc in spc_list:
								try:
									row.append(spc_data[spc][clus]) # this could result in key error if one cluster is completely unoccupied.
								except KeyError:
									row.append('no data')
							writer.writerow(row)
						writer.writerow('')
						# write analysis results
						if len(set(lst_of_clus)) > 2:
							stats = list(spcomp[spc_list[0]].keys())  # ['mean','stdv','ci',...]
							attr = [''] + spc_list
							writer.writerow(attr)
							for stat in stats:
								row = [stat]
								for spc in spc_list:
									row.append(spcomp[spc][stat])
								writer.writerow(row)

						writer.writerow('')


			# grouped SPCOMP data and result
				spc_data = eval(proj_summary_record[spc_grp_data_attr]) # {'SW': {'179': 43.8, '183': 85.7, '190': 80.0, '189': 70.0, '184': 72.7}, 'BF': {'179': 0, '183': 7.1, '190': 10.0, '189': 0, '184': 18.2},...}
				spcomp = eval(proj_summary_record[spcomp_grp_attr]) # {'SW': {'mean': 70.44, 'stdv': 16.1187, 'ci': 20.014, 'upper_ci': 90.454, 'lower_ci': 50.426, 'n': 5, 'confidence': 0.95}, 'BF':...}
				if len(spc_data) > 0:
					spc_list = list(spc_data.keys())
					attr = ['cluster_number'] + spc_list
					with open(csvfilename,'a') as f:
						writer = csv.writer(f, lineterminator='\n')
						writer.writerow(['<SPCOMP (Grouped)>'])
						writer.writerow(attr)
						for clus in lst_of_clus:
							row = [clus]
							for spc in spc_list:
								try:
									row.append(spc_data[spc][clus]) # this could result in key error if one cluster is completely unoccupied.
								except KeyError:
									row.append('no data')
							writer.writerow(row)
						writer.writerow('')
						# write analysis results
						if len(set(lst_of_clus)) > 2:					
							stats = list(spcomp[spc_list[0]].keys())  # ['mean','stdv','ci',...]
							attr = [''] + spc_list
							writer.writerow(attr)
							for stat in stats:
								row = [stat]
								for spc in spc_list:
									row.append(spcomp[spc][stat])
								writer.writerow(row)
						else:
							writer.writerow(['Not enough data to run statistics'])
						writer.writerow('')

			# Site Occupancy:
				attr = ['cluster_number', so_data_attr, so_reason]			
				with open(csvfilename,'a') as f:
					writer = csv.writer(f, lineterminator='\n')
					writer.writerow(['<Site Occupancy>'])
					writer.writerow(attr)
					rows = []
					for clus in lst_of_clus: # do this loop to make sure records are sorted.
						for record in data:
							row = [v for k,v in record.items() if k in attr] # we just need cluster number, number of trees and effective density.
							if row[0] == clus: 
								writer.writerow(row)
								break
					writer.writerow('') # skip one line

				# Site Occupancy calculated result:
				if len(set(lst_of_clus)) > 2:
					proj_data = [record for record in self.proj_summary_dict if record['proj_id']==p]
					results = proj_data[0][so_attr] # eg. {'mean': 0.7708, 'stdv': 0.3826, 'ci': 0.4015, 'upper_ci': 1.1723, 'lower_ci': 0.3693, 'n': 6, 'confidence': 0.95}
					with open(csvfilename,'a') as f:
						writer = csv.writer(f, lineterminator='\n')
						writer.writerow(eval(results).keys())
						writer.writerow(eval(results).values())
						writer.writerow(['SO = 1 for a cluster if at least one tree was surveyed for each and every plot in that cluster.'])
						writer.writerow('')


			# Number of trees and effective density data:
				attr = ['cluster_number', num_tree_attr, eff_dens_attr]			
				with open(csvfilename,'a') as f:
					writer = csv.writer(f, lineterminator='\n')
					writer.writerow(['<EFFECTIVE DENSITY>'])
					writer.writerow(attr)
					rows = []
					for clus in lst_of_clus: # do this loop to make sure records are sorted.
						for record in data:
							row = [v for k,v in record.items() if k in attr] # we just need cluster number, number of trees and effective density.
							if row[0] == clus: 
								writer.writerow(row)
								break
					writer.writerow('') # skip one line

				# Number of trees and effective density calculated result:
				if len(set(lst_of_clus)) > 2:
					proj_data = [record for record in self.proj_summary_dict if record['proj_id']==p]
					p_effect_dens_attr = self.proj_summary_attr['p_effect_dens'] # should give you 'effective_density'
					results = proj_data[0][p_effect_dens_attr] # eg. {'mean': 1979.1667, 'stdv': 1271.9428, 'ci': 1334.8221, 'upper_ci': 3313.9888, 'lower_ci': 644.3446, 'n': 6, 'confidence': 0.95}
					with open(csvfilename,'a') as f:
						writer = csv.writer(f, lineterminator='\n')
						writer.writerow(eval(results).keys())
						writer.writerow(eval(results).values())
						writer.writerow('')


			# Residual data:
				res_raw = eval(proj_summary_record[residual_data_attr]) #eg. {'PB': {'901': 0, '179': 0, '184': 0, '189': 6, '183': 0, '190': 0}, 'SW': {'901': 0, '179': 3, '184': 0, '189': 0, '183': 0, '190': 0},...}
				res_count = eval(proj_summary_record[residual_count_attr]) # {'PB': 6, 'SW': 3, 'MR': 2, 'BF': 13, 'PW': 4}
				res_percent = eval(proj_summary_record[residual_percent_attr]) # {'PB': 21.4, 'SW': 10.7, 'MR': 7.1, 'BF': 46.4, 'PW': 14.3}
				res_BA = eval(proj_summary_record[residual_BA_attr]) # {'PB': 2.0, 'SW': 1.0, 'MR': 0.67, 'BF': 4.33, 'PW': 1.33}

				if len(res_raw) > 0:
					spc_list = list(res_raw.keys())
					with open(csvfilename,'a') as f:
						writer = csv.writer(f, lineterminator='\n')
						writer.writerow(['<RESIDUALS>'])

						# writing raw data					
						attr = ['cluster_number'] + spc_list
						writer.writerow(attr)
						for clus in lst_of_clus:
							row = [clus]
							for spc in spc_list:
								row.append(res_raw[spc][clus])
							writer.writerow(row)

						# writing calc results
						attr = [''] + spc_list + ['Total']
						writer.writerow('')
						writer.writerow(attr)
						# total number of trees
						row = ['total num of trees']
						total_counter = 0
						for spc in spc_list:
							row.append(res_count[spc])
							total_counter += res_count[spc]
						row.append(total_counter)
						writer.writerow(row)
						# percent
						row = ['Percent']
						total_counter = 0
						for spc in spc_list:
							row.append(res_percent[spc])
							total_counter += res_percent[spc]
						row.append(total_counter)
						writer.writerow(row)					
						# BA
						row = ['Basal Area (m2/ha)']
						total_counter = 0
						for spc in spc_list:
							row.append(res_BA[spc])
							total_counter += res_BA[spc]
						row.append(total_counter)
						writer.writerow(row)

						writer.writerow(['Note that BA = total num of trees * 2 / n, where n = total num of clusters'])
						writer.writerow('')

				# if no residual data found:
				else:
					with open(csvfilename,'a') as f:
						writer = csv.writer(f, lineterminator='\n')
						writer.writerow(['<RESIDUALS>'])
						writer.writerow(['No Data Found'])
						writer.writerow([''])


			# Ecosite
				attr = ["cluster_number", "moisture", "nutrient", "comments"]
				ecosite_data = eval(proj_summary_record[ecosite_data_attr]) # {'901': ['dry', '', ''], '190': ['fresh', '', ''], '189': ['fresh', '', ''], '184': ['moist', '', ''], '183': ['fresh', 'Very rich', 'Testing'],...}
				moisture = eval(proj_summary_record[ecosite_moisture_attr]) # {'dry': 16.7, 'fresh': 66.7, 'moist': 16.7}

				with open(csvfilename,'a') as f:
					writer = csv.writer(f, lineterminator='\n')
					writer.writerow(['<ECOSITE>'])
					writer.writerow(attr)
					for clus in lst_of_clus:
						row = [clus] + ecosite_data[clus] # eg. ['901','dry', '', '']
						writer.writerow(row)
				# just the moisture
					writer.writerow('')
					writer.writerow(list(moisture.keys()))
					writer.writerow(list(moisture.values()))
					writer.writerow('')

			# footnote
				footnote = """<FOOTNOTE>
							\nFor SPCOMP calculations, only the %s tallest trees per plot were used,\
							\nbut for Effective Density, all surveyed trees were used in calculation.
							\nHow to calculate standard deviation(stdv) and confidence interval(ci) youself:\
							\nFor stdv, use excel's STDEV.S function over the range (column) of data\
							\nFor ci, calculate stdv first then use excel's CONFIDENCE.T function with alpha = 0.05 and size = n"""%self.calc_max
				with open(csvfilename,'a') as f:
					writer = csv.writer(f, lineterminator='\n')
					for line in footnote.split('\n'):
						writer.writerow([line])

			except PermissionError:
				self.logger.info("!!!!! Error - could not create %s. Check if the file is being used."%csvfilename)


	def run_all(self):
		self.tbl_2_dict()
		self.plot_to_csv()
		self.clus_to_csv()