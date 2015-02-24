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
var path = require("path");
var util = require("util");
var Stream = require("stream");

// The boundary key
var boundaryKey = Math.random().toString(16);
			
function startClient(fileListSource) {

	var options = {
		host: "127.0.0.1",
		port: 8000,
		method: "POST"
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
			process.stdout.write("================================================================================================================\n");
			process.stdout.write("==================================== Right before starting the SENDER! =========================================\n");
			process.stdout.write("================================================================================================================\n");
			
			// Create the sender/client/request
			var sender = http.request(options, responseHandler);
			sender.setHeader("Content-Type", "multipart/form-data; boundary=\"" + boundaryKey + "\"");
			
			// for (var i = 0; i < fileList.length - 1; i++) {
			// Replaced fileList.length with constant 6, to have only 5 files to transfer so I can easily debug; uncomment line above and comment line below for normal run
			for (var i = 0; i < 3; i++) {
				// Get the path to the file about to be transfered
				var sourceFileName = fileList[i]["FileName"];
				var sourceFilePath = fileList[i]["FilePath"];
				var sourceFileMD5 = fileList[i]["MD5"];
				var sourceFileSize = fileList[i]["FileSize"];
				
				readAndPipeFile(i, sourceFilePath, sender);
				
				/* =======================================================================
				// the header for the one and only part (need to use CRLF here)
				sender.write("--" + boundaryKey + "\r\n"
					// use your file's mime type here, if known
					+ "Content-Type: text/plain\r\n" // application/octet-stream
					// "name" is the name of the form field
					// "filename" is the name of the original file
					+ "Content-Disposition: form-data; name=\"my_file\"; filename=\"" + sourceFileName + "\"\r\n"
					+ "Content-Transfer-Encoding: binary\r\n\r\n");
					
				var inputStream = fs.createReadStream(sourceFilePath);//, {buffersize: 4*1024});
				inputStream.on("end", function() {
					sender.write("\r\n--" + boundaryKey + "--\r\n\r\n");
					process.stdout.write("############# FILE PROCESSED @ step " + i + "  ##########################################\n");
					process.stdout.write(sourceFilePath + "\n" + sourceFileSize + "\n" + sourceFileMD5 + "\n");
					process.stdout.write("############### END OF THE REQUEST STREAM #########################################\n");
				});
				inputStream.pipe(sender, {end: false});
				inputStream.on("finish", function() {
					console.error("***************** Everything has been piped. ****************");
				});
				====================================================================*/
			}
			// buffer = sourceFilePath;
			// POST_DATA += "]}";
			// console.log(JSON.stringify(POST_DATA));
			// client.write(JSON.stringify(POST_DATA));//, "utf8");
			/*
			inputStream.on("end", function() {
				sender.end();
			});
			*/
			// sender.end("<<<<<<<<<<<<<<<<<<< SENDER CLOSED >>>>>>>>>>>>>>>>>>>\n");
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
	//process.stdout.write(util.inspect(res.headers));
	process.stdout.write("************ Response content type: " + res.headers["content-type"] + " ***********************************************\n");
	process.stdout.write("************ Response content length: " + res.headers["content-length"] + " ***********************************************\n");
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
			process.stdout.write("************* SERVER HAS BEEN CLOSED *************************\n");
		});
	}
	else {
		console.log("ERROR >>>>> " + res.statusCode);
	};
};

var readAndPipeFile = function(index, sourceFilePath, sender) {
	var inputStream = fs.createReadStream(sourceFilePath);//, {buffersize: 4*1024});
	var outputStream = fs.createWriteStream("./outputFile.txt");
	process.stdout.write("File number " + index + " and path " + sourceFilePath + "\n");
	var ii = 0;
	var iii = 0;
	inputStream.on("data", function(chunk, index) {
		process.stdout.write("------>>>>>> inputStream.ON.DATA for " + path.basename(sourceFilePath) + " <<<<<<<<------------\n");
		// the header for the one and only part (need to use CRLF here)
		sender.write("--" + boundaryKey + "\r\n"
			 // use your file's mime type here, if known
			 + "Content-Type: text/plain\r\n" // application/octet-stream
			 // "name" is the name of the form field
			 // "filename" is the name of the original file
			 + "Content-Disposition: form-data; name=\"file_no_\"" + index + "; filename=\"" + path.basename(sourceFilePath) + "\"\r\n"
			 + "Content-Transfer-Encoding: binary\r\n\r\n", "utf8");
		// sender.pipe(outputStream);
		

		//console.log("at the beginning --->>> " + util.inspect(sender));

		if (!sender.write(chunk, "utf8")) {
			// console.log("inside IF --->>> " + util.inspect(sender));
			process.stdout.write("Chunk processing for " + path.basename(sourceFilePath) + ". Therefore pausing inputStream ..........\n");
			inputStream.pause();
			process.stdout.write("ii = " + ii + ", iii = " + iii + "\n");
			ii += 1;
		} else {
			process.stdout.write("Data didn't have to be chunked!!!" + path.basename(sourceFilePath) + "\n");
			sender.write("--" + boundaryKey + "--\r\n\r\n", "utf8");
			// var outputStream = fs.createWriteStream("./outputFile.txt", "utf8");
			// sender.pipe(outputStream);
			// console.log(sender);
		};
		
		sender.on("drain", function() {
			// console.log("inside DRAIN --->>> " + chunk);
			process.stdout.write("Draining the queued buffer in " + path.basename(sourceFilePath) + "\n");
			sender.write("--" + boundaryKey + "\r\n\r\n");
			inputStream.resume();
			process.stdout.write("ii = " + ii + ", iii = " + iii + "\n");
			iii += 1;
		});
	});
	
	

	inputStream.on("end", function() {
		// sender.write("\r\n--" + boundaryKey + "--\r\n\r\n");
		process.stdout.write("############# FILE PROCESSED  #########################################\n");
		process.stdout.write(sourceFilePath + "\n");
		process.stdout.write("############### END OF THE REQUEST STREAM #########################################\n");
		// var newFile = fs.createWriteStream("./" + path.basename(sourceFilePath));
		// sender.pipe(newFile);
		// sender.end();
	});
	/*
	inputStream.on("finish", function() {
		console.error("***************** Everything has been piped. ****************");
	});
	*/
	// inputStream.pipe(sender, {end: false});
};

module.exports.startClient = startClient;
