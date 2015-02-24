#!/usr/bin/env python3
# that would be the 'shebang' to make this code executable, without always envoking python3
# but first make the script executable with sudo chmod +x *.py
import os
import sys
import json # for dumping the data in JSON format
import hashlib # for calculating the MD5
import operator # for sorting a list of dictionaries based on key
import time

data = []

# Recursive function to scan current folder and its subfolders for files and create
# a list of dictionaries of all files and their specific info. I did it like this
# so I could later sort the list of dictionaries based on dictionary key values
def scanFolder(folderPath):
	global data
	compName = os.listdir(folderPath)
	for inFolder in compName:
		curPath = os.path.abspath(folderPath + '/' + inFolder)
		if os.path.isdir(curPath):
			scanFolder(curPath)
		elif os.path.isfile(curPath):
			# print("current file is: " + curPath) # replace this with a function writing the JSON file path
			fileName = os.path.basename(curPath) # file name
			filePath = curPath # file path
			fileSize = os.path.getsize(curPath) # file size in bytes
			fileCTime = os.path.getctime(curPath) # time of creation (Windows), time of last metadata change (Linux)
			fileMTime = os.path.getmtime(curPath) # last modification time for the file
			# calculating the MD5
			# When READ function is called, it reads and loads the file into memory. It is dangerous when not sure about the size
			# because it could result in wrong MD5. Therefore, it is advisable to read chunks of the file and constantly update the MD5.
			BLOCKSIZE = 65536
			hashMD5 = hashlib.md5()
			with open(filePath, 'rb') as aFile:
				buf = aFile.read(BLOCKSIZE)
				while len(buf) > 0:
					hashMD5.update(buf)
					buf = aFile.read(BLOCKSIZE)
			fileMD5 = hashMD5.hexdigest()
			# Create the dictionary for each file
			info = [('FileName', fileName), \
					('FilePath', filePath), \
					('FileSize', fileSize), \
					('CreationTimeSec', fileCTime), \
					('CreationTime', time.ctime(fileCTime)), \
					('LastModTimeSec', fileMTime), \
					('LastModTimeL', time.ctime(fileMTime)), \
					('MD5', fileMD5)]
			data.append(dict(info))
	return data

folderToScan = os.path.abspath('../File_samples')
if os.path.exists(folderToScan):
	print("Folder " + folderToScan + " exists, so I am going to scan it for files!!!!\n")
	# ==================================================================================
	jsonFileName = 'output.json'
	jsonFilePath = os.path.join(os.getcwd(), jsonFileName)
	if os.path.exists(jsonFilePath):
		os.remove(jsonFilePath)
		print("I've removed the old file... I hope... and will create a new one\n")
		jsonFile = open(jsonFilePath, 'a')
		jsonFile.write("{\n\t\"Source\": \"" + jsonFilePath + "\",\n\t\"FileList\": [\n")
	else:
		print("No need to remove the old file, 'cause there's none\n")
		jsonFile = open(jsonFilePath, 'a')
		jsonFile.write("{\n\t\"source\":", jsonFilePath, ",\n\t\"desc\": [")
#	data = scanFolder(folderToScan, jsonFile)
	data = scanFolder(folderToScan)
	# Sort the data based on the last modification time
	sortedData = sorted(data, key = operator.itemgetter('CreationTimeSec'))

	for ind in range(len(sortedData)):
		# Write to JSON file, separated by comma
#		jsonFile.write("\t\t")
		jsonFile.write(json.dumps(sortedData[ind], sort_keys = True, indent = "\t\t"))
		if ind < len(sortedData) - 1:
			jsonFile.write(',\n')
		# print(sortedData[ind]['Last modification time'], data[ind]['Last modification time'])
	jsonFile.write("\n\t]\n}")
	jsonFile.close()
else:
	print("Check the path you want me to scan for files, because " + folderToScan + " does not seem to exist.\n")
