# this script is used when running the TDT tool and SEM analysis script together

import os, time
from TDT import executeTDT
import SEM


# running TDT tool
# if you need to configure TDT tool, then you need to edit TDT/executeTDT.py or TDT/TDT.cfg file.
tdt_msg = executeTDT.main() #eg. if failed: returns -1, if success: C:\DanielK_Work\OfficeWork\Temp\raw_data\RAP_project_2020-07-13_4.zip

if tdt_msg == -1:
	custom_datapath = None
	initial_msg = "TDT failed to download projects from Terraflex inSphere server"

else:
	custom_datapath = os.path.join(tdt_msg[:-4],'data') # eg. C:\DanielK_Work\OfficeWork\Temp\raw_data\RAP_project_2020-07-13_4\data
	initial_msg = "TDT download successful!\nDownload path: %s"%custom_datapath


# running SEM.py tool
configfile = r'D:\ACTIVE\HomeOffice\RAP\script\SEM.cfg'
SEM.sem(configfile,initial_msg,custom_datapath, ignore_testdata = True)
print('SEM.py run successfully!!')
time.sleep(10)


