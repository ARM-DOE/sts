#!/usr/bin/env sh
':' //; exec "$(command -v nodejs || command -v node)" "$0" "$@"

// Name:
//		startSending - parses the funtion arguments to get the file used as a data input for the client
// Synopsis:
//		startSending [option] [file path]
// Available options:
//		--f, -file
//			to specify the JSON file that contains the database with files to be transferred
// Author: Laurentiu Dan Marinovici
// Pacific Northwest National Laboratory, Richland, WA
// Last update: 2015-02-12

var http = require("http");
var fs = require("fs");
var path = require("path");
var httpClient = require("./startHTTPclientJSON");

// Following line clear the treminal window and places the cursor at position (0, 0)
process.stdout.write("\u001B[2J\u001B[0;0f");

// Function to parse the arguments, and take specific actions
function parseInputArgs() {
	var fileListSource;
	// Get the argument list
	var inputArgs = process.argv;
	
	for (var i = 0; i < inputArgs.length; i++) {
		var currArg = inputArgs[i];
		if (currArg === "--f" || currArg === "-file") {
			if (inputArgs[i + 1]) {
				fileListSource = inputArgs[i + 1];
				process.stdout.write("The input file is : " + fileListSource + "\n");
			}
			else {
				process.stdout.write("A file name should be specified!\n");
			}
		}
	}
	return fileListSource;
}


var fileListSource = parseInputArgs();
fs.exists(fileListSource, function(exists) {
	if (exists) {
		process.stdout.write(path.resolve(fileListSource) + "\n");
		httpClient.startClient(path.resolve(fileListSource));
	}
});
