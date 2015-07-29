#!/usr/bin/env python3
# that would be the 'shebang' to make this code executable, without always envoking python3
# but first make the script executable with sudo chmod +x *.py
import os
import sys
import getopt		# for being able to supply input arguments to the script
import json 		# for dumping the data in JSON format
import hashlib 		# for calculating the MD5
import operator 	# for sorting a list of dictionaries based on key
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

# The main function
def main(argv):
	folderToScan = ''
	jsonFileName = ''
	try:
		opts, args = getopt.getopt(argv, "hi:o:", ["ifolder=", "ofile="])
		if not opts:
			print("ERROR: need options and arguments to run.")
			print("Usage: ./createSortedJSONfile.py -i <inputFolderToScan> -o <jsonOutputFileName>")
			sys.exit()
	except getopt.GetoptError:
		print("Wrong option or no input argument! Usage: ./createSortedJSONfile.py -i <inputFolderToScan> -o <jsonOutputFileName>")
		sys.exit(2)
	for opt, arg in opts:
		if  opt == "-h":
			print("Help prompt. Usage: ./createSortedJSONfile.py -i <inputFolderToScan> -o <jsonOutputFileName>")
			sys.exit()
		elif opt in ("-i", "--ifolder"):
			folderToScan = arg
		elif opt in ("-o", "--ofile"):
			jsonFileName = arg
	if os.path.exists(folderToScan):
		print("Folder " + os.path.abspath(folderToScan) + " exists, so I am going to scan it for files!!!!\n")
		# ==================================================================================
		jsonFilePath = os.path.join(os.getcwd(), jsonFileName)
		if os.path.exists(jsonFilePath):
			os.remove(jsonFilePath)
			print("I've removed the old file... I hope... and will create a new one\n")
			jsonFile = open(jsonFilePath, 'a')
			jsonFile.write("{\n\t\"Source\": \"" + os.path.abspath(jsonFilePath) + "\",\n\t\"FileList\": [\n")
		else:
			print("No need to remove the old file, 'cause there's none\n")
			jsonFile = open(jsonFilePath, 'a')
			jsonFile.write("{\n\t\"Source\": \"" + os.path.abspath(jsonFilePath) + "\",\n\t\"FileList\": [\n")
	#	data = scanFolder(folderToScan, jsonFile)
		data = scanFolder(os.path.abspath(folderToScan))
		# Sort the data based on the last modification time
		sortedData = sorted(data, key = operator.itemgetter('CreationTimeSec'))

		for ind in range(len(sortedData)):
			# Write to JSON file, separated by comma
			# jsonFile.write("\t\t")

			jsonFile.write(json.dumps(sortedData[ind], sort_keys=True, indent=2))
			if ind < len(sortedData) - 1:
				jsonFile.write(',\n')
			# print(sortedData[ind]['Last modification time'], data[ind]['Last modification time'])
		jsonFile.write("\n\t]\n}")
		jsonFile.close()
	else:
		print("Check the path you want me to scan for files, because " + folderToScan + " does not seem to exist.\n")
		sys.exit()

if __name__ == "__main__":
	main(sys.argv[1:]) 	# sys.argv gets the list of the command line arguments, but we have to trip down the first argument
						# which represents the name of the actual script being run
