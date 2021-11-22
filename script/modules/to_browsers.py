# Purpose:
# To create html, css and js that has maps, summaries and other info and exprot them to the user specified folder
# More specifically, the data comes from sqlite database and is being translated to tables and maps in html/css/js format.
# this script uses the html/css/js template scripts in the "browser_template" folder
# this module comes after analysis.py module.

import sqlite3, os, shutil

# importing custom modules
if __name__ == '__main__':
	import common_functions, html_to_aspx
else:
	from modules import common_functions, html_to_aspx



class To_browsers:
	def __init__(self, cfg_dict, db_filepath, logger):
		self.db_filepath = db_filepath
		self.logger = logger
		self.clus_summary_tblname = cfg_dict['SQLITE']['clus_summary_tblname']
		self.proj_summary_tblname = cfg_dict['SQLITE']['proj_summary_tblname']
		self.projects_shp = cfg_dict['SHP']['shp2sqlite_tablename']
		self.dst_path = os.path.join(cfg_dict['OUTPUT']['outputfolderpath'], 'browser')
		self.report_doc_path = cfg_dict['PDF']['report_folder']
		self.ref_doc_path = cfg_dict['PDF']['ref_folder']

		self.logger.info("\n")
		self.logger.info("--> Running to_browsers module")
		self.timenow = common_functions.datetime_readable() #eg. Apr 21, 2020. 02:09 PM


	def tbl_2_dict(self):
		"""Turns sqlite tables into list of dictionaries"""
		self.clus_summary_dict = common_functions.sqlite_2_dict(self.db_filepath, self.clus_summary_tblname)
		self.proj_summary_dict = common_functions.sqlite_2_dict(self.db_filepath, self.proj_summary_tblname)

		# get project ids that actually has any data collected
		self.active_projs = [record['proj_id'] for record in self.proj_summary_dict if int(record['num_clusters_surveyed']) > 0]
		self.logger.info("Active Projects: %s"%self.active_projs)


	def move_templates(self):
		""" copy and paste the template html/css/js from the 'browser_template' folder to the destination folder"""

		# move the html/css/js files from the browser_template folder to the destination folder
		self.logger.debug("copying html/css/js files over to %s"%self.dst_path)
		if __name__ == '__main__':
			src_path = 'browser_template'
		else:
			src_path = r'modules\browser_template'

		# if dst_path doesn't already exist, create one.
		if not os.path.isdir(self.dst_path):
			try:
				os.mkdir(self.dst_path)
			except OSError:
				self.logger.info("Creation of the directory %s failed"%self.dst_path)		

		for root, dirs, files in os.walk(src_path):
			for f in files:
				src_file = os.path.join(root,f)
				if root == src_path:
					dst_folder = self.dst_path
				else:
					child_folder = os.path.split(root)[1]
					dst_folder = os.path.join(self.dst_path,child_folder)
					if not os.path.isdir(dst_folder): os.mkdir(dst_folder)
				shutil.copy(src_file,dst_folder)


		self.logger.debug("successfully copied html/css/js files over")

		# full paths of the files
		self.cssfile = os.path.join(self.dst_path,'lib','RAP_init.css')
		self.htmlfile = os.path.join(self.dst_path,'index.html')
		self.jsfile = os.path.join(self.dst_path,'lib','RAP_init.js')

		self.p_cssfile = os.path.join(self.dst_path,'proj','Proj.css')
		self.p_htmlfile = os.path.join(self.dst_path,'proj','Proj_template.html')
		self.p_jsfile = os.path.join(self.dst_path,'proj','Proj.js')

		# quick check if all these files are good to go
		for i in [self.cssfile, self.htmlfile, self.jsfile, self.p_cssfile, self.p_htmlfile, self.p_jsfile]:
			if not os.path.exists(i):
				self.logger.info("Could not locate %s.   Check to_browsers.py's move_templates module"%i)
				raise Exception("Could not locate %s.   Check to_browsers.py's move_templates module"%i)

	def create_dashboard_table(self):
		"""creates a new table (dashboard) in sqlite database then populates the dashboard browser's table section.
		This is done by searching and replacing $$Table%% word in the RAP_init_html file with the actual table script
		"""
		self.logger.debug("running create_dashboard_table method")
		self.dash_table = "dashboard"
		sql = """
			CREATE TABLE %s AS 
			SELECT proj_id AS "Project ID", 
			spatial_FMU AS "FMU", 
			spatial_MNRF_district AS "District", 
			num_clusters_surveyed||" of "||num_clusters_total AS "Clusters Surveyed",
			assess_last_date AS "Last Survey Date",
			ROUND(area_ha,1) AS "Area ha",
			silvsys AS "SILVSYS",
			ROUND(lat,5) AS lat,
			ROUND(lon,5) AS lon
			FROM %s
			ORDER BY "Last Survey Date" DESC;
		"""%(self.dash_table, self.proj_summary_tblname)

		con = sqlite3.connect(self.db_filepath)
		cur = con.cursor()
		cur.execute(sql)

		# Turn the sqlite table into html string
		html = common_functions.sqlite_2_html(self.db_filepath, self.dash_table, query=None, rename_header = {})

		# replace the project id with an anchor tag.
		for proj in self.active_projs:
			path_to_project_html = os.path.join('proj',create_html_filename(proj)) # eg. proj\p_JVBonhomme.html
			a_tag = '<a href="%s">%s</a>'%(path_to_project_html, proj)
			html = html.replace(proj,a_tag)


		# rewrite the index.html to include the table.
		common_functions.replace_txt_in_file(txtfile = self.htmlfile, being_replaced='$$Table%%', replacing_with=html)

		# also write time
		common_functions.replace_txt_in_file(txtfile = self.htmlfile, being_replaced='$$time_now%%', replacing_with=self.timenow)


	def create_dashboard_map(self):
		"""Creates the map portion of the index.html's dashboard by editing lib/RAP_init.js.
		for each project a popup pinpoint will be created on the map.
		eg. L.marker([51.5, -0.09]).addTo(mymap).bindPopup("<b>Hello world!</b><br />I am a popup.");
		"""
		self.logger.debug("running create_dashboard_map method")

		# convert the table into a list of dictionaries because it's easier to work with
		self.dash_table = common_functions.sqlite_2_dict(sqlite_db_file=self.db_filepath, tablename=self.dash_table)

		# open the js file
		f = open(self.jsfile,'a')

		# append the js script
		js_script = "\n//markers for each project\n"
		for proj in self.dash_table:
			js_script += """L.marker([%s, %s]).addTo(mymap).bindPopup('<strong>%s</strong><br>Surveyed: %s<br>%sha');"""%(
						proj['lat'],proj['lon'],proj['Project ID'],proj['Clusters Surveyed'],proj['Area ha'])
			js_script += "\n"

		# replace the project id with an anchor tag.
		for proj in self.active_projs:
			path_to_project_html = r'proj\\' + create_html_filename(proj) # eg. proj\\p_JVBonhomme.html - note that for js, you need '\\'
			a_tag = '<a href="%s">%s</a>'%(path_to_project_html, proj)
			js_script = js_script.replace(proj,a_tag)

		f.write(js_script)
		f.close()


	def create_proj_pages1(self):
		"""Creates one html file per project within browser/proj folder
		This module focuses on the title and the map. (not the tables or pictures)
		"""
		self.logger.debug("running create_proj_pages1 method")

		# grab the string in the template html file
		with open(self.p_htmlfile,'r') as f:
			template_str = f.read()

		# creating and modifying html files
		for proj in self.active_projs:
			# Creating html files
			newfilename = create_html_filename(proj) # this function is at the bottom of this script
			newfilepath = os.path.join(self.dst_path, 'proj', newfilename)

			# grabbing project info
			proj_info_dict = [info for info in self.dash_table if info['Project ID'] == proj][0] # eg. {"Project ID": "JVBonhomme", "lat": "48.232", ...}
			clus_surveyed = proj_info_dict['Clusters Surveyed']
			proj_lat = str(proj_info_dict['lat'])
			proj_lon = str(proj_info_dict['lon'])

			# replacing strings
			new_str = template_str.replace("$$ProjectID%%",proj)
			new_str = new_str.replace("$$time_now%%",self.timenow)
			new_str = new_str.replace("$$Progress%%",clus_surveyed)
			new_str = new_str.replace("48.8888", proj_lat) # 48.8888 is the template's latitude
			new_str = new_str.replace("-83.3333", proj_lon)

			# writing the html file
			with open(newfilepath, 'w') as f:
				f.write(new_str)

			# Editing the map: editing proj.js file
			js_script = "\n//markers for each cluster in %s\n"%proj
			# grabbing cluster info from cluster summary table
			clus_summary_list = [info for info in self.clus_summary_dict if info['proj_id'] == proj] # eg. [{"cluster_number" = '703', 'site_occ' = '0.75'}...,]
			for clus_summary in clus_summary_list:
				clus_lat = clus_summary['lat']
				clus_lon = clus_summary['lon']

				# Note that starting Dec 2020, if the user have not collected lat long, the X, Y value will be blank instead of 0, 0.
				# so we are setting lat lon as 0, 0 manually here - otherwise the javascript will crash.
				if clus_lat == '': clus_lat = 0 
				if clus_lon == '': clus_lon = 0

				proj_clus_name = clus_summary['proj_id'] + '-' + clus_summary['cluster_number']
				so = clus_summary['site_occ']
				spc_comp = clus_summary['spc_comp_perc'].replace("'","")
				date = clus_summary['creation_date']

				js_script += """L.marker([%s, %s]).addTo(mymap).bindPopup('<strong>%s</strong><br>Site Occ: %s<br>SPCOMP: %s<br>Date: %s');"""%(
							clus_lat, clus_lon, proj_clus_name, so, spc_comp, date)
				js_script += "\n"

			with open(self.p_jsfile,'a') as f:
				f.write(js_script)


	def create_proj_pages2(self):
		"""Reads each projectid.html file and adds Summary section.
		This method is all about replacing $$Summary%% text in each of the projectid.html files.
		"""
		self.logger.debug("running create_proj_pages2 method")

		# modifying html files
		for proj in self.active_projs:
			# getting html filesnames
			newfilename = create_html_filename(proj) # this function is at the bottom of this script
			htmlfilepath = os.path.join(self.dst_path, 'proj', newfilename)

			# grabbing project info
			proj_info_dict = [info for info in self.dash_table if info['Project ID'] == proj][0] # eg. {"Project ID": "JVBonhomme", "lat": "48.232", ...}
			proj_sum_dict = [summary for summary in self.proj_summary_dict if summary['proj_id'] == proj][0] # eg. {"proj_id": "JVBonhomme", "sfl_so" = '0.9', ...}

			proj_surveyed = proj_info_dict['Clusters Surveyed'] # eg. '9 of 30'
			proj_latlon = '%s, %s'%(proj_info_dict['lat'], proj_info_dict['lon']) # eg. '48.49337, -81.34618'
			proj_area = proj_sum_dict['area_ha']
			proj_start_date = proj_sum_dict['assess_start_date']
			proj_last_date = proj_info_dict['Last Survey Date']
			proj_assessors = proj_sum_dict['assessors']
			proj_comments = proj_comments_summary(proj_sum_dict['all_comments'])

			html = """\n\n<table id="noline">
			<tr><td><strong>Project ID:</strong></td> 					<td>{0}</td></tr>
			<tr><td><strong>Clusters Surveyed:</strong></td> 			<td>{1}</td></tr>
			<tr><td><strong>Project Location (lat, lon): </strong></td> <td>{2}</td></tr>
			<tr><td><strong>Project Area:</strong></td> 				<td>{3} ha</td></tr>
			<tr><td><strong>Date First Surveyed:</strong></td> 			<td>{4}</td></tr>
			<tr><td><strong>Date Last Surveyed:</strong></td> 			<td>{5}</td></tr>
			<tr><td><strong>Surveyed by:</strong></td> 					<td>{6}</td></tr>
			<tr><td><strong>Comments: </strong></td>					<td>{7}</td></tr>
			</table>\n""".format(proj, proj_surveyed, proj_latlon, proj_area, proj_start_date, proj_last_date, proj_assessors, proj_comments)

			# SFL's data
			sfl_as_yr = str(proj_sum_dict['sfl_as_yr'])
			sfl_spcomp = proj_sum_dict['sfl_spcomp']
			sfl_so = str(proj_sum_dict['sfl_so'])
			sfl_fu = proj_sum_dict['sfl_fu']
			sfl_effden = str(proj_sum_dict['sfl_effden'])

			html += """\n<br><br>
			<table id="striped" class="sfl">
				<caption><strong>SFL's Assessment</strong>
				</caption>
			<tr>
				<th>SFL Assessment Year</th>			
				<th>SFL SPCOMP</th>
				<th>SFL Site Occ</th>
				<th>SFL Forest Unit</th>
				<th>SFL Effective Density</th>
			</tr>
			<tr>
				<td>{0}</td>
				<td>{1}</td>
				<td>{2}</td>
				<td>{3}</td>
				<td>{4}</td>
			</tr>
			</table><br>\n""".format(sfl_as_yr, sfl_spcomp, sfl_so, sfl_fu, sfl_effden)


			# MNRF assessment summary
			# SPCOMP
			spcomp = eval(proj_sum_dict['spcomp']) # eg {'BF': {'mean': 7.06, 'stdv': 7.6229, 'ci': 9.465, 'upper_ci': 16.525, 'lower_ci': -2.405, 'n': 5, 'confidence': 0.95}, 'CB':...}
			enough_data, html_script = spcomp_to_html_table(spcomp, 'MNRF SPCOMP') # this function is at the bottom of this script
			html += html_script

			# SPCOMP (grouped)
			if enough_data:
				spcomp_grp = eval(proj_sum_dict['spcomp_grp'])
				html += spcomp_to_html_table(spcomp_grp, 'MNRF SPCOMP (grouped)')[1]

			# Site occupancy and effective density
			so = eval(proj_sum_dict['site_occupancy']) # eg. {'mean': 0.7708, 'stdv': 0.3826, 'ci': 0.4015, 'upper_ci': 1.1723, 'lower_ci': 0.3693, 'n': 6, 'confidence': 0.95}
			ed = eval(proj_sum_dict['effective_density']) # eg. {'mean': 1979.1667, 'stdv': 1271.9428, ...}
			html += so_ed_to_html_table(so,ed, "MNRF Site Occupancy and Effective Density")[1]

			# Ecosite moisture
			ecosite = eval(proj_sum_dict['ecosite_moisture']) # eg. {'fresh': 66.7, 'moist': 16.7, 'dry': 16.7}
			html += ecosite_to_html_table(ecosite, 'MNRF Ecosite Moisture')[1]

			common_functions.replace_txt_in_file(txtfile = htmlfilepath, being_replaced='$$Summary%%', replacing_with=html)

	def create_proj_pages3(self):
		"""Reads each projectid.html file and adds rest of the sections - Processed Data, Raw Data, and Pictures
		This method is all about replacing $$ProcessedData%%, $$RawData%%, and $$Pictures%% text in each of the projectid.html files.
		"""
		self.logger.debug("running create_proj_pages3 method")

		# modifying html files
		for proj in self.active_projs:
			# getting html filesnames
			newfilename = create_html_filename(proj) # this function is at the bottom of this script
			htmlfilepath = os.path.join(self.dst_path, 'proj', newfilename)
			html = ''

			# ### Processed Data Section ###
			# grabbing processed data from the sqlite database (z_... tables)
			tablename = common_functions.create_proj_tbl_name(proj)
			html += common_functions.sqlite_2_html(self.db_filepath, tablename)

			common_functions.replace_txt_in_file(txtfile = htmlfilepath, being_replaced='$$ProcessedData%%', replacing_with=html)


			# # ### Raw Data Section ###
			# html = ''
			# sql = """SELECT cluster_number, creation_date, spc_count, total_num_trees, effective_density, site_occ, 
			# 		site_occ_reason, cluster_comments, ecosite_moisture, ecosite_nutrient, ecosite_comment, lat, lon
			# 		FROM Cluster_Summary WHERE proj_id = '%s'
			# 		ORDER BY cluster_number"""%proj
			# html += common_functions.sqlite_2_html(sqlite_db_file = self.db_filepath, tablename = 'Cluster_Summary', query=sql)
			# common_functions.replace_txt_in_file(txtfile = htmlfilepath, being_replaced='$$RawData%%', replacing_with=html)


			### Photos Section ###
			html = ''
			clus_sum_dict = [summary for summary in self.clus_summary_dict if summary['proj_id'] == proj] # records in Cluster_Summary table where the record's proj_id matches with current proj_id
			clus_lst = [int(clus['cluster_number']) for clus in clus_sum_dict]
			clus_lst.sort()

			# extra layer of complexity here to make sure the cluster numbers are sorted
			for sorted_clus in clus_lst:
				for clus in clus_sum_dict:
					clus_num = int(clus['cluster_number'])
					if sorted_clus == clus_num:
						clus_comments = eval(clus['cluster_comments']) # eg. {'cluster': '', 'ecosite': '', 'P1': '', 'P2': '', 'P3': '', ... 'P8': ''}
						photo_path = eval(clus['sharepoint_photopath']) # eg. {'cluster': ['https://ontariogov.sharepoint.com/:i:/r/sites/MNRF-ROD-EXT/RAP/RAP%20Picture%20Library/2021/CHA-MISS-2021-BLK-17_C73_cluster_8e67_2021-10-28.jpg'], 'P1': [], 'P2': [], ... 'P7': [], 'P8': []}

						for location, path_lst in photo_path.items():
							if len(path_lst) > 0:
								comm = "C%s %s photo. %s"%(clus_num, location, clus_comments[location])
								for path in path_lst:
									html += make_photo_gallery(path, comm)


			if html == '': html = 'No photos were taken.'
			common_functions.replace_txt_in_file(txtfile = htmlfilepath, being_replaced='$$Pictures%%', replacing_with=html)



	def add_supp_doc(self):
		""" copies what was in the "pdf_to_post" to a new folder in the output path.
		creates hyperlinks to each pdf files.
		These hyperlinks will replace the $$Doc%% text in the RAP Report's Document tab.
		"""
		self.logger.debug("running add_supp_doc method")

		# create a new folder called 'pdf'
		rel_path = 'pdf'
		pdf_path = os.path.join(self.dst_path,rel_path)
		if not os.path.isdir(pdf_path):
			try:
				os.mkdir(pdf_path)
			except OSError:
				self.logger.info("Creation of the directory %s failed"%pdf_path)

		# copying the pdf documents over.
		# first get the list of files in the pdf_to_post folder.
		report_files = os.listdir(self.report_doc_path)
		ref_files = os.listdir(self.ref_doc_path)
		report_pdfs = [file.upper() for file in report_files if file.upper().endswith('.PDF')]
		ref_pdfs = [file.upper() for file in ref_files if file.upper().endswith('.PDF')]

		# Writing html docs
		html = '<h3>Reports</h3>'
		if len(report_pdfs) > 0:
			self.logger.debug("copying over %s report pdf files"%len(report_pdfs))
			# if there are any pdf file exist then copy them over
			for pdf in report_pdfs:
				pdf_old_path = os.path.join(self.report_doc_path, pdf)
				pdf_new_path = os.path.join(pdf_path, pdf)
				shutil.copy(pdf_old_path, pdf_new_path)
				link_text = pdf[:-4]
				html += """<br><a href="%s/%s" target="_blank">%s</a>"""%(rel_path, pdf, link_text)
		else:
			# some default html string stating that there are no documents to show
			self.logger.debug("no report pdf file to copy over")
			html += """<br>No Reports to show"""

		html += '<br><br><h3>Reference Documents</h3>'
		if len(ref_pdfs) > 0:
			self.logger.debug("copying over %s ref pdf files"%len(ref_pdfs))
			# if there are any pdf file exist then copy them over
			for pdf in ref_pdfs:
				pdf_old_path = os.path.join(self.ref_doc_path, pdf)
				pdf_new_path = os.path.join(pdf_path, pdf)
				shutil.copy(pdf_old_path, pdf_new_path)
				link_text = pdf[:-4]
				html += """<br><a href="%s/%s" target="_blank">%s</a>"""%(rel_path, pdf, link_text)
		else:
			# some default html string stating that there are no documents to show
			self.logger.debug("no re pdf file to copy over")
			html += """<br>No Reports to show"""


		common_functions.replace_txt_in_file(txtfile = self.htmlfile, being_replaced='$$Doc%%', replacing_with=html)
			

	def add_log(self):
		log_msg = self.logger.info_msg
		common_functions.replace_txt_in_file(txtfile = self.htmlfile, being_replaced='$$Log%%', replacing_with=log_msg)



	def create_aspx_files(self):
		""" sharepoint doesn't take html files but takes aspx files to display html language.
		This is bit more work than just changing file name extensions. for more info, read html_to_aspx.py file.
		"""
		browser_folder_path = self.dst_path # path to the /browser folder
		new_folder_path = os.path.join(os.path.split(browser_folder_path)[0],'browser_aspx')
		html_to_aspx.main(browser_folder_path, new_folder_path)


	def run_all(self):
		self.tbl_2_dict()
		self.move_templates()
		self.create_dashboard_table()
		self.create_dashboard_map()
		self.create_proj_pages1()
		self.create_proj_pages2()
		self.create_proj_pages3()
		# self.add_supp_doc()
		self.add_log()
		self.create_aspx_files()

##############    End of class "To_browsers"   ######################









def create_html_filename(projectID):
	html_filename = 'p_' + common_functions.no_special_char(projectID) + '.html'
	return html_filename


def proj_comments_summary(proj_comments):
	return_str = ""
	all_comments = eval(proj_comments)
	# eg.{'27': {'cluster': '', 'ecosite': '', 'P1': '', 'P2': '', 'P3': '', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''}, 
	#   '14': {'cluster': '', 'ecosite': '', 'P1': 'No FTG due to residual spruce', 'P2': '', 'P3': '', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''},...}
	if len(all_comments) > 1:
		for clus, comment_dict in all_comments.items():
			for plot, comment in comment_dict.items():
				if len(comment) > 0:
					return_str += "Cluster %s %s: %s<br>"%(clus, plot, comment)
	if len(return_str)>4:
		return_str = return_str[:-4]
	return return_str



def spcomp_to_html_table(spcomp, title):
	"""spcomp should be a dictionary in this format:  
	{'BF': {'mean': 7.06, 'stdv': 7.6229, 'ci': 9.465, 'upper_ci': 16.525, 'lower_ci': -2.405, 'n': 5, 'confidence': 0.95}, 'CB': {'mean': 2.5, 'stdv': 5.5902, 'ci': 6.9411,..}
	"""
	html = ''
	enough_data = True
	spc_list = sorted(spcomp.keys()) # eg. ['AB', 'BF', 'CE', 'PB', 'SW'...]
	if len(spcomp) == 0 or len(spcomp[spc_list[0]]) == 0:
		html += "\n<p>Not enough data to evaluate species composition</p>\n"
		enough_data = False
	else:
		header = [''] + spc_list
		rows = spcomp[spc_list[0]].keys() # ['mean','stdv','ci','upper_ci',...]
		html += """\n<br><br>
			<table id="striped">
				<caption><strong>%s</strong>
				</caption>
			<tr>\n"""%title

		# first, we write the header
		for head in header:
			html += "<th>%s</th>"%head
		html += "</tr>\n"

		# write each row
		for row in rows:
			html += "<tr>"
			for col_num, spc in enumerate(header):
				if col_num == 0:
					html += "<td><strong>%s</strong></td>"%row # eg. "mean"
				else:
					if row == 'mean':
						value = round(spcomp[spc][row],1) # eg. 13.0
					else:
						value = spcomp[spc][row] # eg. 13.0121
					html += "<td>%s</td>"%value
			html += "</tr>\n"
		html += "</table><br>\n"
	return [enough_data, html]


def so_ed_to_html_table(so, ed, title):
	"""site occupancy and effective density must be in the following format:
	{'mean': 0.7708, 'stdv': 0.3826, 'ci': 0.4015, 'upper_ci': 1.1723, 'lower_ci': 0.3693, 'n': 6, 'confidence': 0.95}
	"""
	html = ''
	enough_data = True
	if len(so) == 0 or len(ed) == 0:
		html += "\n<p>Not enough data to evaluate Site Occupancy and Effective Density</p>\n"
		enough_data = False
	else:
		header = ['', 'Site Occupancy', 'Effective Density']
		rows = so.keys() # ['mean','stdv','ci','upper_ci',...]
		html += """\n<br><br>
			<table id="striped">
				<caption><strong>%s</strong>
				</caption>
			<tr>\n"""%title

		# first, we write the header
		for head in header:
			html += "<th>%s</th>"%head
		html += "</tr>\n"

		# write each row
		for row in rows:
			html += "<tr>"
			for column in header:
				if column == '':
					html += "<td><strong>%s</strong></td>"%row # eg. "mean"
				elif column == 'Site Occupancy':
					val = so[row]
					html += "<td>%s</td>"%val
				elif column == 'Effective Density':
					val = ed[row]
					html += "<td>%s</td>"%val					
			html += "</tr>\n"
		html += "</table><br>\n"
	return [enough_data, html]


def res_to_html_table(res_count, res_percent, res_BA, title = "MNRF Residuals"):
	""" Creates MNRF residuals summary table
	example of res_count = {'BF': 13, 'PB': 6, 'PW': 4, 'MR': 2, 'SW': 3}
	"""
	html = ''
	enough_data = True
	if len(res_count) == 0 or len(res_percent) == 0 or len(res_BA) == 0:
		html += "\n<p>No residual data available</p>\n"
		enough_data = False
	else:
		# add totals
		for d in [res_count, res_percent, res_BA]:
			tot = round(sum(list(d.values())),1)
			d['zztotal'] = tot ## to make sure total goes to the end.

		# values for the header and rows
		header = [''] + sorted(list(res_count.keys()))
		rows = ['Residual Tree Count','Residual Tree Percent','Basal Area']
		html += """\n<br><br>
			<table id="striped">
				<caption><strong>%s</strong>
				</caption>
			<tr>\n"""%title

		# first, we write the header
		for head in header:
			html += "<th>%s</th>"%head
		html += "</tr>\n"

		# write each row
		for row in rows:
			html += "<tr>"
			for spc in header:
				if spc == '':
					html += "<td><strong>%s</strong></td>"%row # eg. 'Residual Tree Count'
				else:
					if row == 'Residual Tree Count':
						val = res_count[spc]
						html += "<td>%s</td>"%val
					elif row == 'Residual Tree Percent':
						val = res_percent[spc]
						html += "<td>%s</td>"%val		
					elif row == 'Basal Area':
						val = res_BA[spc]
						html += "<td>%s</td>"%val								
			html += "</tr>\n"
		html += "</table><br>\n"
	html = html.replace("zztotal", 'Total')
	return [enough_data, html]	


def ecosite_to_html_table(ecosite, title='MNRF Ecosite Moisture'):
	""" an example of ecosite = {'fresh': 66.7, 'moist': 16.7, 'dry': 16.7}
	"""
	html = ''
	enough_data = True
	if len(ecosite) == 0:
		html += "\n<p>Ecosite has not been evaluated</p>\n"
		enough_data = False
	else:
		header = list(ecosite.keys())
		html += """\n<br><br>
			<table id="striped">
				<caption><strong>%s</strong>
				</caption>
			<tr>\n"""%title

		# first, we write the header
		for head in header:
			html += "<th>%s</th>"%head
		html += "</tr>\n"

		# write one row
		html += "<tr>"		
		for head in header:
			html += "<td>{0} %</td>".format(ecosite[head])
		html += "</tr>\n</table><br>\n"
	return [enough_data, html]


def make_photo_gallery(href, desc):

	html = """\n
<div class="gallery">
  <a target="_blank" href="{0}">
    <img src="{0}" alt="{1}">
  </a>
  <div class="desc">{1}</div>
</div>\n""".format(href, desc)
	return html













if __name__ == '__main__':
	spcomp_example = {'BF': {'mean': 7.06, 'stdv': 7.6229, 'ci': 9.465, 'upper_ci': 16.525, 'lower_ci': -2.405, 'n': 5, 'confidence': 0.95}, 
	'CB': {'mean': 2.5, 'stdv': 5.5902, 'ci': 6.9411, 'upper_ci': 9.4411, 'lower_ci': -4.4411, 'n': 5, 'confidence': 0.95}, 
	'LA': {'mean': 11.32, 'stdv': 15.3976, 'ci': 19.1187, 'upper_ci': 30.4387, 'lower_ci': -7.7987, 'n': 5, 'confidence': 0.95}, 
	'BW': {'mean': 7.42, 'stdv': 12.9916, 'ci': 16.1312, 'upper_ci': 23.5512, 'lower_ci': -8.7112, 'n': 5, 'confidence': 0.95}, 
	'SW': {'mean': 70.44, 'stdv': 16.1187, 'ci': 20.014, 'upper_ci': 90.454, 'lower_ci': 50.426, 'n': 5, 'confidence': 0.95}, 
	'BN': {'mean': 1.24, 'stdv': 2.7727, 'ci': 3.4428, 'upper_ci': 4.6828, 'lower_ci': -2.2028, 'n': 5, 'confidence': 0.95}}

	# print(spcomp_to_html_table(spcomp_example, 'Test'))

	proj_comm = {'27': {'cluster': '', 'ecosite': '', 'P1': '', 'P2': '', 'P3': '', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''}, 
	'14': {'cluster': '', 'ecosite': '', 'P1': 'No FTG due to residual spruce', 'P2': '', 'P3': '', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''}, 
	'7': {'cluster': '', 'ecosite': '', 'P1': '', 'P2': '', 'P3': '', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''}, 
	'15': {'cluster': '', 'ecosite': '', 'P1': '', 'P2': '', 'P3': '', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''}, 
	'28': {'cluster': '', 'ecosite': '', 'P1': '', 'P2': '', 'P3': '', 'P4': '', 'P5': '', 'P6': '', 'P7': '', 'P8': ''}}

	print(proj_comments_summary(proj_comm))