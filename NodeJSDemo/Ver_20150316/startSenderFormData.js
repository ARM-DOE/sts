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
// Modified: 2015-02-12
//	Changes:
//		- adding the MD5 to the request stream in sourceFileMD5
// Last working update: 2015-03-16
//  Changes:
//      - The code has been modified to have the METADATA in JSON string format to reduce overhead.
//		- Also, I have eliminated most of the control printouts to make code easier to read.

var http = require("http");
// var http = require("https");
var fs = require("fs");
var path = require("path");
var util = require("util");
var Stream = require("stream");
var FormData = require("form-data");
var form = new FormData();

// process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

function startClient(fileListSource) {

	var options = {
		hostname: "127.0.0.1", // "market.pnl.gov", // "130.20.71.13", // 
		port: 8000,
		method: "POST",
		path: "/upload",
		headers: form.getHeaders()
	};

	// Read the list of files that need to be tranfered
	fs.readFile(fileListSource, "utf8", function(err, data) {
		if (err) {
			process.stdout.write("err = " + err + " , so throwing error\n");
			throw err;
		};
		var jsonObj = JSON.parse(data);
		if (jsonObj.hasOwnProperty("FileList")) {
			var fileList = jsonObj["FileList"];
			process.stdout.write("=========================================================\n");
			process.stdout.write("=========== Right before starting the SENDER! ===========\n");
			process.stdout.write("=========================================================\n");
			
			// Create the sender/client/request
			var sender = http.request(options, responseHandler);
			var outputStream = fs.createWriteStream("./outputFile.txt");
			for (var i = 0; i < fileList.length - 1; i++) {
			// Replaced fileList.length with constant 6, to have only 5 files to transfer so I can easily debug; uncomment line above and comment line below for normal run
			// for (var i = 0; i < 2; i++) {
			 	// process.stdout.write(util.inspect(fileList[i].FileName) + "\n");
			 	var jsonMetaString = JSON.stringify(fileList[i]);
				// Get the path to the file about to be transfered
				var sourceFilePath = fileList[i]["FilePath"];
				form.append("METADATA", jsonMetaString);
				form.append("FileContent", fs.createReadStream(sourceFilePath));
			}
			// form.pipe(outputStream);
			form.pipe(sender);
		}
		else {
			process.stdout.write("The JSON object does have the property you are looking for!\n");
		}
	});
}; // END OF THE startClient exported function

function responseHandler(res) {
	process.stdout.write("******************************************************\n");
	process.stdout.write("********* Inside the CLIENT callback!!! **************\n");
	process.stdout.write("******************************************************\n");
	process.stdout.write("******** I got a response from the server! ***********\n");
	process.stdout.write("****** Status code: " + res.statusCode + " ***********\n");
	process.stdout.write("******************************************************\n");
	process.stdout.write("*** Response content type: " + util.inspect(res.headers["content-type"]) + " ****\n");
	process.stdout.write("***** Response content length: " + util.inspect(res.headers["content-length"]) + " ******\n");
	res.setEncoding("utf8");
	if (res.statusCode < 399) {
		var txt = "";
		res.on("data", function(chunk) {
			txt += chunk;
			if (res.statusCode === 200) {
				process.stdout.write("Message from server: \n" + chunk);
			}
		});
		res.on("end", function(data) {
			process.stdout.write("****** SERVER HAS FINISHED WITH THE TRANSFER! *****\n");
		});
	}
	else {
		console.log("ERROR >>>>> " + res.statusCode);
	};
};

module.exports.startClient = startClient;
