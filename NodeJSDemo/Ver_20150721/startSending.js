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
// Last update: 2015-07-06
//   Purpose: - add the maximum upload size required as an input argument
//            - run as : startSending.js -f <file path> -u <upload size in kB>

var http = require("http");
var fs = require("fs");
var path = require("path");
// var httpClient = require("./startHTTPclientJSON");
// var httpClient = require("./startHTTPclientJSON1");
// var httpClient = require("./startSenderFormData");
var httpClient = require("./startSenderFormDataPartFile");
// var httpClient = require("./startCoreHTTPSenderPartFile");
var colors = require("colors");

// Set color theme
colors.setTheme({
  silly:           "rainbow",
  input:           "grey",
  verbose:         "cyan",
  prompt:          "grey",
  info:            "green",
  data:            "grey",
  help:            "cyan",
  warn:            "yellow",
  debug:           "blue",
  error:           "red",
  formFile:        "yellow",
  formFileBegin:   "yellow",
  formField:       "red",
  formEnd:         "cyan"
});

// Following line clear the treminal window and places the cursor at position (0, 0)
process.stdout.write("\u001B[2J\u001B[0;0f");

// Function to parse the arguments, and take specific actions
function parseInputArgs() {
	var fileListSource;
	var uploadSize;
	// Get the argument list
	var inputArgs = process.argv;
	if (inputArgs.length <= 2) {
		process.stdout.write(colors.error("Need some arguments!\nRun using startSending.js -f <file path> -u <upload size in kB> \n"));
		process.exit(1);
	}
	else{
		for (var i = 0; i < inputArgs.length; i++) {
			var currArg = inputArgs[i];
			if (currArg === "-f" || currArg === "--file") {
				if (inputArgs[i + 1] && (inputArgs[i + 1] !== "-u" || inputArgs[i + 1] !== "--upload")) {
					// I assume the argument after -f is a file; actually, that should be checked before proceeding
					fileListSource = inputArgs[i + 1];
					process.stdout.write("The input file is : " + fileListSource + "\n");
				}
				else {
					process.stdout.write(colors.error("A file path/name should be specified!\nRun using startSending.js -f <file path> -u <upload size in kB> \n"));
					process.exit(1);
				}
			}
			else if (currArg === "-u" || currArg === "--upload") {
			  if (inputArgs[i + 1] && (inputArgs[i + 1] !== "-f" || inputArgs[i + 1] !== "--file")) {
			    uploadSize = inputArgs[i + 1];
			    process.stdout.write("The maximum upload size should be " + uploadSize + " kB.\n");
			  }
			  else {
			    process.stdout.write(colors.red("A maximum size in kilo bytes for the transfer band width should be specified!\nRun using startSending.js -f <file path> -u <upload size in kB> \n"));
			    process.exit(1);
			  }
			}
			else if (currArg === "-h" || currArg === "--help") {
				process.stdout.write(colors.verbose("You've reached the HELP line!\nRun using startSending.js -f <file path> -u <upload size in kB> \n"));
				process.exit(0);
			}
		}
	}
	return [fileListSource, uploadSize];
}


var args = parseInputArgs();
var fileListSource = args[0];
var uploadSize = args[1];

if (fileListSource == undefined) {
	console.log("File List Source is a required argument. Specify with -f or --file option".error);
	process.exit();
}

if (uploadSize == undefined) {
	console.log("Upload Size is a required argument. Specify with -u or --upload option".error);
	process.exit();
}

fs.exists(fileListSource, function(exists) {
	if (exists) {
		process.stdout.write(path.resolve(fileListSource) + "\n");
		httpClient.startClient(path.resolve(fileListSource), uploadSize);
	}
});
