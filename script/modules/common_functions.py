

def rand_alphanum_gen(length):
	"""
	Generates a random string (with specified length) that consists of A-Z and 0-9.
	"""
	import random, string
	return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))


def datetime_stamp():
	import datetime
	dateTimeObj = datetime.datetime.now()
	timestampStr = dateTimeObj.strftime("%y%m%d%H%M%S") #eg 200421140227
	return timestampStr

def datetime_readable(dtype=1):
	import datetime
	dateTimeObj = datetime.datetime.now()
	if dtype==1:
		timestampStr = dateTimeObj.strftime("%b %d, %Y. %I:%M %p") # eg. Apr 21, 2020. 02:09 PM
	return timestampStr


def no_special_char(input_str):
	"""
	replace all special characters and spaces with an underscore
	"""
	import re
	new_str = re.sub('[^a-zA-Z0-9]', '_', input_str)
	return new_str

def unpack_cfg(cfg_file):
	import configparser

	config = configparser.ConfigParser()
	config.read(cfg_file)

	return config

def cfg_to_dict(cfg_file):
	import configparser
	parser = configparser.ConfigParser()
	parser.read(cfg_file)

	cfg_dict = {section: dict(parser.items(section)) for section in parser.sections()}
	return cfg_dict



def open_spc_group_csv(spc_group_csv_file):
	"""
	processes the species group csv file. 
	in this csv the first column should have a list of unique species code, and the 2nd column should have the species group cooresponding to the species.
	returns a list of variables: [spc_in_csv, grp_2_spc_dict]
	spc_in_csv is a list of unique species found in the first column of the csv file.
	grp_2_spc_dict is a dictionary where keys are the species groups (2nd column) and the values are a list of species from the 1st column that fits into that group.
	"""

	import csv
	# a list of all species in the FRI tech spec:
	all_spc = ['AB', 'AW', 'AX', 'BD', 'BE', 'BF', 'BG', 'BN', 'BW', 'BY', 'CB', 'CD', 'CE', 'CH', 'CR', 'CW', 'EW', 'EX', 'HE', 'HI', 'IW', 'LA', 'LO', 'MH', 'MR', 'MS', 'MX', 'OC', 'OH', 'OR', 'OW', 'OX', 'PB', 'PD', 'PJ', 'PL', 'PO', 'PR', 'PS', 'PT', 'PW', 'PX', 'SB', 'SW', 'SX', 'WB', 'WI', 'AL', 'AQ', 'AP', 'AG', 'BC', 'BP', 'GB', 'BB', 'CAT', 'CC', 'CM', 'CP', 'CS', 'CT', 'ER', 'EU', 'HK', 'HT', 'HL', 'HB', 'HM', 'HP', 'HS', 'HC', 'KK', 'LE', 'LJ', 'BL', 'LL', 'LB', 'GT', 'MB', 'MF', 'MM', 'MT', 'MN', 'MP', 'AM', 'EMA', 'MO', 'OBL', 'OB', 'OCH', 'OP', 'OS', 'OSW', 'PA', 'PN', 'PP', 'PC', 'PH', 'PE', 'RED', 'SS', 'SC', 'SK', 'SN', 'SR', 'SY', 'TP', 'HAZ']

	spc_in_csv = [] # ['SB','SW',...]
	grp_in_csv = [] # ['SX','SX',...]
	temp_copy_of_csv = []  # [['Sb', 'Sx'],['Sw', 'Sx'],...]
	num_of_rows = 0
	with open(spc_group_csv_file, newline='') as csvfile:
		reader = csv.reader(csvfile)
		attributes = next(reader) # the first line
		for row in reader:
			temp_copy_of_csv.append(row)
			spc_in_csv.append(row[0].upper())
			grp_in_csv.append(row[1].upper())
			num_of_rows += 1

	# checking if the list of species is a unique list.
	if len(spc_in_csv) != len(set(spc_in_csv)):
		raise Exception('Error in SpeciesGroup.csv. Duplicate species code found in the first column.')

	# checking if any of the values are left blank
	if '' in spc_in_csv or '' in grp_in_csv:
		raise Exception('Error in SpeciesGroup.csv. Empty space(s) found in the first or second column.')

	# checking if unacceptable species code was used
	for spc in spc_in_csv:
		if spc not in all_spc:
			raise Exception('Error in SpeciesGroup.csv. "%s" is not an acceptable spcies.'%spc)

	# group: spcies dictionary (all uppercase)
	grp_2_spc_dict = {i[1].upper():[] for i in temp_copy_of_csv}
	for i in temp_copy_of_csv:
		grp_2_spc_dict[i[1].upper()].append(i[0].upper())

	# print(grp_2_spc_dict) # {'BF': ['BF'], 'BW': ['BW'], 'CE': ['CE'], 'LA': ['LA'], 'PO': ['PO'], 'PT': ['PT'], 'SX': ['SB', 'SW']}
	return [spc_in_csv, grp_2_spc_dict]



def sqlite_2_dict(sqlite_db_file, tablename):
	import sqlite3

	con = sqlite3.connect(sqlite_db_file)
	con.row_factory = sqlite3.Row
	c = con.cursor()
	c.execute('SELECT * FROM %s'%tablename)

	result = [dict(row) for row in c.fetchall()]
	# print(result)
	con.close()
	
	return result

def create_proj_tbl_name(proj_id, prefix = 'z_'):
	"""input the project id and it will output a project table name
	special characters will be replaced by "_" and it will have a prefix of z_.
	"""
	name = prefix + no_special_char(proj_id)
	return name



def select_tallest_x(trees_in_plotx = [['La', 1.0], ['La', 2.0], ['La', 0.8], ['Sw', 3.0]], max_selected = 3):
	"""
	the default is given as an example.
	Input: list of list of species code and its corresponding height.
	Output: returns the tallest 3 trees in the same format as the input.
	based on the default values given, the return should be [['La', 1.0], ['La', 2.0], ['Sw', 3.0]]
	If there's a tie, the tree with lower index number (tree that was recorded first) in the input list will be included.
	This function assumes that there's no non-numeric values in the height field.
	"""
	heights = [v[1] for v in trees_in_plotx]   # [1.0, 2.0, 0.8, 3.0]
	heights.sort(reverse=True)  # [3.0, 2.0, 1.0, 0.8]
	if len(heights) > max_selected:
		heights = heights[:max_selected]

	selected_trees = []
	for i in heights:
		for j in trees_in_plotx:
			if j[1] == i:
				selected_trees.append(j)
				break
	
	return selected_trees


def dict_lst_to_sqlite(dict_lst, db_filepath, new_tablename, logger):
	"""
	create a new table in the sqlite database and populate it with the list of dictionaries given
	Only works on list of dictionaries where all dictionaries has the same list of keys.
	eg. [{'id':1, 'name':'daniel'},{'id':2, 'name':'sam'}]
	"""
	import sqlite3

	logger.info('Running dict_lst_to_sqlite() to create a new table called %s'%new_tablename)
	# Initiating Connection
	logger.debug('Initiating connection with the sqlite database')
	con = sqlite3.connect(db_filepath)
	cur = con.cursor()

	# dict_lst shouldn't be an empty list
	rec_count = len(dict_lst)
	if rec_count < 1:
		err_msg = '!!!! %s is empty!!!!!'%dict_lst
		logger.info(err_msg)
		raise Exception(err_msg)

	# get a list of attr names
	attr_names = dict_lst[0].keys()

	# the sql script for creating a new table
	create_t_sql = "CREATE TABLE %s "%new_tablename
	str_attr_names = '('
	for f in attr_names:
		str_attr_names += f + ','
	str_attr_names = str_attr_names[:-1] # to remove the trailing comma
	str_attr_names += ")" # this str_attr_names will be used again later
	create_t_sql += str_attr_names + ";"
	# example:
	# CREATE TABLE projects_shp 
	# (OBJECTID,ProjectID,Area_ha,MNRF_AsMet,PlotSize_m,YRDEP,DepletionF,TargetFU,SILVSYS,SGR,FMU,District,SFL_SPCOMP,SFL_SiteOc,SFL_FU,SFL_EffDen,SFL_AsMeth,SFL_Name,SHAPE_Leng,SHAPE_Area);

	# run create table query
	try:
		cur.execute(create_t_sql)
		logger.debug(create_t_sql)
	except:
		logger.info("* WARNING: Table '%s' already exists. Dropping and recreating the table."%new_tablename)
		cur.execute("DROP TABLE %s"%new_tablename)	
		cur.execute(create_t_sql)
		logger.debug(create_t_sql)	

	# inserting values
	insert_sql = "INSERT INTO %s %s"%(new_tablename, str_attr_names)
	row_counter = 0
	for row in dict_lst:
		values_sql = " VALUES ("
		for v in row.values():
			val = str(v)
			val = val.replace('"',"'") # replace " by '
			val = '"' + val + '"' # enclose by double quote to avoid SQL syntax error.
			values_sql += val + ', '
		values_sql = values_sql[:-2] # delete trailing comma and a space
		values_sql += ");"
		sql = insert_sql + values_sql
		# example:
		# INSERT INTO projects_shp (OBJECTID,ProjectID,Area_ha,MNRF_AsMet,PlotSize_m,YRDEP,DepletionF,TargetFU,SILVSYS,SGR,FMU,District,SFL_SPCOMP,SFL_SiteOc,SFL_FU,SFL_EffDen,SFL_AsMeth,SFL_Name,SHAPE_Leng,SHAPE_Area) 
		# VALUES ('1', 'BuildingSouth', '8.41044', 'Ground', '8', '0', '', '', 'CC', '', '', '', '', '0.0', '', '0', 'Aerial', '', '0.0159852651688', '1.02960591055e-05');
		logger.debug(sql)
		cur.execute(sql)
		row_counter += 1
	if row_counter == rec_count:
		logger.info("%s rows have been successfully transferred to %s."%(row_counter, new_tablename))
	else:
		logger.info("!!! not all rows have been transferred into %s !!!"%new_tablename)

	# Closing Connection
	logger.debug('Closing connection with the sqlite database')
	con.commit()
	con.close()


def sort_integers(lst):
	"""
	provided a list of integers (and text) in text format eg ['710','702','701','700'] or ['k','702','701','700', 'e','f']
	Returns a sorted list of integers (and text) in text format. eg. ['700', '701', '702', '710'] or ['700', '701', '702', 'e', 'f', 'k']
	if none-integer found, will keep it in the original text form.
	"""
	lst_int = []
	lst_str = []
	for i in lst:
		try:
			lst_int.append(int(i))
		except:
			lst_str.append(i)
	lst_int.sort()
	lst_str.sort()

	new_lst = lst_int + lst_str
	new_lst = [str(i) for i in new_lst]

	return new_lst



def sqlite_2_html(sqlite_db_file, tablename, query=None, rename_header = {}, table_id="example"):
	"""turns a sqlite table into a string that you can use to create a table in html
	optionally you can include sqlite query, and add a dictionary to replace attribute names.
	for example, if rename_header = {'proj_id': 'Project ID'}, then proj_id attribute name "proj_id" will be replaced by "Project ID"
	"""
	import sqlite3

	con = sqlite3.connect(sqlite_db_file)
	con.row_factory = sqlite3.Row
	c = con.cursor()
	if query == None:
		c.execute('SELECT * FROM %s'%tablename)
	else:
		c.execute(query)

	result = [dict(row) for row in c.fetchall()]
	attrs = result[0].keys() # list of attributes

	# rename attrs
	if len(rename_header) > 0:
		for k, v in rename_header.items():
			for index, item in enumerate(attrs):
				if item == k:
					attrs[index] = v

	html = '<table id="{0}" class="display" style="width:100%">'.format(table_id)

    # table head
	html += "\n<thead>\n<tr>"
	for attr in attrs:
		html += "\n<th>%s</th>"%attr
	html += "\n</tr></thead>"

	# table body
	html += "\n<tbody>"
	for row in result:
		html += "\n<tr>"
		for val in row.values():
			html += "\n<td>%s</td>"%val
		html += "\n</tr>"
	html += "\n</tbody>"

	html+= "\n</table>"

	return html


def replace_txt_in_file(txtfile, being_replaced, replacing_with):
	"""edits any txt file (txt,cfg,html,css,js) by replacing a string with another string
	"""
	# txtfile = r'C:\DanielK_Work\OfficeWork\Temp\browser\RAP_init.html'

	# read
	with open(txtfile,'r') as f:
		html_script = f.read()

	# replace
	html_script = html_script.replace(being_replaced,replacing_with)

	# write
	with open(txtfile,'w') as f:
		f.write(html_script)


if __name__ == '__main__':
	# print(datetime_stamp())
	print(datetime_readable())


	# print(no_special_char('W!#3 4)4320OPw{>'))

	

	# cfg = unpack_cfg(r'C:\DANIEL~1\SEM\Script\SEM.cfg')
	# print(cfg['CSV']['csvfolderpath'])



	# cfg = cfg_to_dict(r'C:\DANIEL~1\SEM\Script\SEM.cfg')
	# print(cfg)


	# spc_group_csv_file = r'C:\Users\kimdan\OneDrive - Government of Ontario\SEM\script\SpeciesGroup.csv'
	# open_spc_group_csv(spc_group_csv_file)


	# db = r'C:\DanielKimWork\temp\SEM_NER_200211092459.sqlite'
	# tablename = 'l387081_Cluster_Survey_Testing_'
	# sqlite_2_dict(db,tablename)


	# t1 = [['La', 1.0], ['La', 1.0], ['La', 1.0], ['Sw', 1.0]]
	# t2 = [['La', 2.0], ['La', 2.0], ['Sw', 1.0], ['La', 1.0]]
	# t3 = [['La', 1.0], ['La', 2.0], ['Sw', 1.0], ['La', 4.0], ['La', 1.0]]
	# t4 = [['La', 1.0], ['La', 2.0], ['Sw', 1.0], ['La', 2.0], ['La', 1.0], ['Sw', 1.0]]

	# for t in [t1,t2,t3,t4]:
	# 	print(select_tallest_x(t))

	# lst1 = ['710','702','701','700']
	# lst2 = ['302c', '303','304','301']
	# lst3 = ['11a','12','15','11b']
	# lst4 = ['k','702','701','700', 'e','f']
	# for l in [lst1,lst2,lst3,lst4]:
	# 	print(sort_integers(l))


	# txtfile = r'C:\DanielK_Work\OfficeWork\Temp\browser\RAP_init.html'
	# being_replaced = '$$Table%%'
	# replacing_with = 'Tada'
	# replace_txt_in_file(txtfile, being_replaced, replacing_with)


	# sqlite_db_file = r'C:\DanielK_Work\OfficeWork\Temp\sqlite\SEM_NER_200709142120.sqlite'
	# tablename = 'Project_Summary'
	# query = """SELECT proj_id, num_clusters_total, num_clusters_surveyed, area_ha, spatial_FMU, spatial_MNRF_district, assessment_date, assessors FROM Project_Summary"""
	# html = sqlite_2_html(sqlite_db_file, tablename, query)
	# print(html)

	print(create_proj_tbl_name('Some Special Project#1'))