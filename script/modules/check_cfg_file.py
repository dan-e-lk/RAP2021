
def check_paths(cfg_dict):
	""" checks paths in the SEM.cfg file to make sure the paths exist.
		Throws warnings if the paths don't exist.
		the cfg_dict has to be in the dictionary form rather than the cfg form.
	"""
	import os

	paths_to_check = {}
	paths_to_check['html - output_folder']= ([cfg_dict['html']['output_folder']])
	paths_to_check['CSV - csvfolderpath']= ([cfg_dict['CSV']['csvfolderpath']])
	paths_to_check['CSV - dbfolderpath']= ([cfg_dict['CSV']['dbfolderpath']])
	paths_to_check['CSV - output_csv_folderpath']= ([cfg_dict['CSV']['output_csv_folderpath']])
	paths_to_check['PDF - pdf_folder']= ([cfg_dict['PDF']['pdf_folder']])

	# eg. paths_to_check= {'html - output_folder': ['D:\\ACTIVE\\HomeOffice\\RAP_outputs\\browser'], 'CSV - csvfolderpath': ['D:\\ACTIVE\\HomeOffice\\RAP_Outputs\\raw_data\\Connect_RAP_21-Dec-20_01-12\\data'], ..}

	error_only = {}
	for k, v in paths_to_check.items():
		if os.path.exists(v[0]):
			paths_to_check[k].append('exists')
		else:
			new_v = v + ['PATH NOT FOUND']
			paths_to_check[k] = new_v
			error_only[k] = new_v

	print(paths_to_check)
	print(error_only)






if __name__ == '__main__':
	# testing...
	import common_functions
	config_file = r'D:\ACTIVE\HomeOffice\RAP\script\SEM.cfg'
	cfg_dict = common_functions.cfg_to_dict(config_file)

	check_paths(cfg_dict)


