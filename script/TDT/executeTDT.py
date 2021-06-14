

import os, sys, time, subprocess
from zipfile import ZipFile


#Run and obtain results for the TDT tool
class TDT():

	#Initialize the class object: TDT
	#required fields: programName, username, password, project, download, saveName
	#Returns: None
	def __init__(self, progName, progUser, progPass, progProj, progDwld, progSave,
					progDbg='False', progGui='False', progExpt='CSV', progData='All',
					progImgs='True', progMkex='False', progDel='False'):
		#progImgs=True means hyperlink =true

		#Message container object
		self.msg = ''
		self.findMsg = ''

		#Pre-defined error messages
		self.pdem01 = "[-1] Unable to find the TDT program. Please ensure that it exists."
		self.pdem02 = '[-2] There are missing required fields. Please ensure you provide the'\
						'necessary values: ProgramName, Username, Password, Project, Download, SaveName.'

		#Store the required argument
		self.progName = progName
		self.progUser = progUser
		self.progPass = progPass
		self.progProj = progProj
		self.progDwld = progDwld
		self.progSave = progSave

		#validate the required fields
		if (self.missingRequiredFields() == True):
			print (self.pdem02)
			sys.exit(-2)

		#validate the program exists
		if (self.verifyTDTTool(self.progName) == False):
			print (self.pdem01)
			sys.exit(-1)

		#Arugments: Non-required fields; provides default values
		self.progDbg = progDbg
		self.progGui = progGui
		self.progExpt = progExpt
		self.progData = progData
		self.progImgs = progImgs
		self.progMkex = progMkex
		self.progDel = progDel

		self.saveNameZip = None

		#Create a path for the zipfile
		self.filePath = None


	#Update the message object
	#Returns: None
	def updateMsg(self, msgInput):
		self.msg = msgInput

	#Append to the message object; must already be initialized
	#Returns: None
	def appendMsg(self, msgInput):
		self.msg += msgInput

	#Update the findMessage object
	#Returns: None
	def updateFindMsg(self, msgInput):
		self.findMsg = self.msg.find(msgInput)

	#Obtain the saveName string from the message object
	#Returns: String
	def obtainSaveName(self):
		lIndex = self.msg.rfind('zip name: ')
		rIndex = self.msg.find('.zip')
		return self.msg[(lIndex+10):rIndex]

	#Update the save name with the zip extension
	#Returns: None
	def updateSaveNameZip(self, saveName):
		self.saveNameZip = '{}.zip'.format(saveName)

	#Create the filepath with the real save name, appended date if required
	#Returns: None
	def createFilePath(self):
		self.filePath = '{}\\{}'.format(self.progDwld, self.saveNameZip)

	#Returns the filePath object; used for the zip process
	#Returns: String
	def getFilePath(self):
		return self.filePath

	#Returns the download location object; used for the zip process
	#Returns: String
	def getDownloadLocation(self):
		return self.progDwld

	#Creates a list object with relevant information for user validation
	#Returns: List
	def returnTDTObjects(self):
		return [self.msg, self.findMsg, self.saveNameZip, self.filePath]

	#Determine if any required field is missing; flag for error
	#Returns boolean
	def missingRequiredFields(self):
		flg = 0
		self.updateMsg('Missing Fields: ')
		if (self.progName == None):
			self.appendMsg('ProgramName ')
			flg = 1
		elif (self.progUser == None):
			self.appendMsg('Username ')
			flg = 1
		elif (self.progPass == None):
			self.appendMsg('Password ')
			flg = 1
		elif (self.progProj == None):
			self.appendMsg('ProjectName ')
			flg = 1
		elif (self.progDwld == None):
			self.appendMsg('DownloadLocation ')
			flg = 1
		elif (self.progSave == None):
			self.appendMsg('SaveName ')
			flg = 1

		#return true if missing required field
		if (flg == 1): return True
		else: return False

	#Unzip the contents of the downloaded file
	#Returns: None
	def unzipDownload(self):
		dlLoc = self.getFilePath()
		dlPath = self.getDownloadLocation()
		print ('\t{}->{}'.format(dlLoc, dlPath))

		try:
			with ZipFile(dlLoc, 'r') as zipObj:
				# Extract all the contents of zip file in current directory
				zipObj.extractall(dlPath)
		except Exception as e:
			print ('Unable to extract the folder...')
			print (e)

	#Validate the location of the TDT tool program; Ensure it is accessible
	#Returns: boolean
	def verifyTDTTool(self, progName):
		#does the TDT tool exist?
		if (os.path.exists(progName)): return True
		else: return False

	#Run the TDT tool; use the arguments provided by the user
	#Returns: None
	def executeDownloadTool(self):

		#Create a list of arguments that the user can call to update the TDT tool
		#callArg = ["TDT.exe", "-uUsername", "-pPassword", "-jProjectName", "-lC:\\Users\\useraccount\\Desktop"]
		##    -u: username
		##    -p: password
		##    -j: project
		##    -l: download format
		##    -s: savename
		##    -t: export type
		##    -d: data type
		##    -i: hyperlinks
		##    -e: mark as exported
		##    -x: delete records
		##    -b: debug mode
		##    -g: GUI mode

		callArg = [self.progName, '-u{}'.format(self.progUser), '-p{}'.format(self.progPass),
					'-j{}'.format(self.progProj), '-l{}'.format(self.progDwld), '-s{}'.format(self.progSave),
					'-t{}'.format(self.progExpt), '-d{}'.format(self.progData), '-i{}'.format(self.progImgs),
					'-e{}'.format(self.progMkex), '-x{}'.format(self.progDel), '-b{}'.format(self.progDbg),
					'-g{}'.format(self.progGui)]

		#Validate that the TDT tool exists in the location
		if (not self.verifyTDTTool(self.progName)):
			print (self.pdem01)
			sys.exit(-1)

		#Perform a suprocess execution of the TDT tool
		p = subprocess.Popen(callArg, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
		out, err = p.communicate()

		#Store the output to the class object msg and findMsg for reference
		self.updateMsg(out.decode("utf-8"))
		self.updateFindMsg('[0]')

		#obtain the save name with datestamp, zip
		self.updateSaveNameZip(self.obtainSaveName())
		self.createFilePath()

		#Return the message to the calling object
		return self.returnTDTObjects()


#END OF CLASS TDT


#Main function; use as an example for the class object intilization
def main():
	#Create the TDT tool class object
	progName = r'D:\ACTIVE\HomeOffice\RAP\script\TDT\TDT.exe'
	progUser = 'mnrf.ner@ontario.ca'
	progPass = 'Roads2017!'
	progProj = 'NER-RAP'
	progDwld = r'D:\ACTIVE\HomeOffice\RAP_outputs\raw_data' # This folder must already exist!!
	progSave = 'RAP_project'
	tdt = TDT(progName, progUser, progPass, progProj, progDwld, progSave, progExpt = 'CSV', progImgs='True')

	print ('[Executing the TDT script...]')
	#Execute the TDT Tool program
	msg = tdt.executeDownloadTool()

	#Return 0 (all good) or -1 for error to calling object
	if (msg[1] > -1):
		#Unzipping the contents
		print ('\t...UnZipping the content')
		tdt.unzipDownload()
		print ('TDT tool ran successfully!!')
		return msg[3]
	else:
		print("something went wrong while running executeTDT.py")
		return -1

if __name__ == "__main__" :
	main()


#END OF MAIN
