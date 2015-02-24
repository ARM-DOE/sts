//#!/usr/bin/env sh
//':' //; exec "$(command -v nodejs || command -v node)" "$0" "$@"
// The "shebang" lines are supposed to hellp with running the script in any environment
// A two-line polyglot shebang (Bourne shell + Node.js
// =====================================================================================

// HTTP Client Module
// Name:
//		startHTTPclientJSON - connects to server and sneds the files according to the JSON data file
// Synopsis:
//		clientObjectName.startClient(JSON file path)
// Author: Laurentiu Dan Marinovici
// Pacific Northwest National Laboratory, Richland, WA
// Last update: 2015-02-12
//	Changes:
//		- adding the MD5 to the request stream in sourceFileMD5

var http = require("http");
var fs = require("fs");
var path = require("path")

function startClient(fileListSource) {

	var options = {
		host: "127.0.0.1",
		port: 8000,
		method: "PUT"
	};

	// Read the list of files that need to be tranfered
	fs.readFile(fileListSource, "utf8", function(err, data) {
		if (err) {
			process.stdout.write("err = ' + err + ' , so throwing error\n");
			throw err;
		};
		var jsonObj = JSON.parse(data);
		if (jsonObj.hasOwnProperty("FileList")) {
			var fileList = jsonObj["FileList"];
			process.stdout.write("================================================================================================================\n");
			process.stdout.write("==================================== Right before starting the CLIENT! =========================================\n");
			process.stdout.write("================================================================================================================\n");

			// Create the client/request
			var client = http.request(options, responseHandler);
			
			// for (var i = 0; i < fileList.length - 1; i++) {
			// Replaced fileList.length with constant 6, to have only 5 files to transfer so I can easily debug; uncomment line above and comment line below for normal run
			for (var i = 0; i < 3; i++) {
				// Get the path to the file about to be transfered
				var sourceFilePath = fileList[i]["FilePath"];
				var sourceFileMD5 = fileList[i]["MD5"];
				process.stdout.write(sourceFilePath + "\t" + sourceFileMD5 + "\n");
				client.write(sourceFilePath, "utf8");
			}
		
			client.end();
		}
		else {
			process.stdout.write("The JSON object does have the property you are looking for!\n");
		}
	});
}; // END OF THE startClient exported function

function responseHandler(res) {
	process.stdout.write("********************************************************************************************************************\n");
	process.stdout.write("*************************************** Inside the CLIENT callback!!! **********************************************\n");
	process.stdout.write("********************************************************************************************************************\n");
	process.stdout.write("************ I got a response from the server! ************* Status code: " + res.statusCode + " **************\n");
	process.stdout.write("********************************************************************************************************************\n");
	res.setEncoding("utf8");
	res.on("data", function(chunk) {
		process.stdout.write("Message from server: " + chunk + "\n");
	});
};

module.exports.startClient = startClient;
