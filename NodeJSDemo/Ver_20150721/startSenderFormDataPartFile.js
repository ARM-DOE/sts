//#!/usr/bin/env sh
//':' //; exec "$(command -v nodejs || command -v node)" "$0" "$@"
// The "shebang" lines are supposed to hellp with running the script in any environment
// A two-line polyglot shebang (Bourne shell + Node.js
// =====================================================================================

// HTTP Client Module
// Name:
//    startHTTPclientJSON - connects to server and sneds the files according to the JSON data file
// Synopsis:
//    clientObjectName.startClient(JSON file path)
// Author: Laurentiu Dan Marinovici
// Pacific Northwest National Laboratory, Richland, WA
// Modified: 2015-02-12
//  Changes:
//    - adding the MD5 to the request stream in sourceFileMD5
// Modified: 2015-03-16
//  Changes:
//      - The code has been modified to have the METADATA in JSON string format to reduce overhead.
//    - Also, I have eliminated most of the control printouts to make code easier to read.
// Last update; 2015-05-21
//  Changes:
//      - need to calculate the MD5 for each chunk of a big file that has been split for transfer, and add this MD5 to the form header to be sent for validation pusposes
var http = require("http");
// var http = require("https");
var fs = require("fs");
var path = require("path");
var util = require("util");
var Stream = require("stream");
var crypto = require("crypto");
var Q = require("q");
var FormData = require("form-data");
var form = new FormData();
var colors = require("colors");
var format = require('string-format');
format.extend(String.prototype)

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
	formEnd:         "cyan",
	log:             "magenta"
});

// process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

function startClient(fileListSource, uploadSize) {
	var CRLF = "\r\n";
	var formOpt = {
		header: "{0}--{1}{0}X-Custom-Header: 123{0}{0}".format(CRLF, form.getBoundary()),
		knownLength: 1
	};
	var options = {
		hostname: "127.0.0.1", // "market.pnl.gov", // "130.20.71.13", //
		port: 8000,
		method: "POST",
		path: "/upload",
		headers: form.getHeaders()
	};
	try {
		// Read the list of files that need to be tranfered
		fs.readFile(fileListSource, "utf8", function(err, data) {
			if (err) {
				process.stdout.write(colors.error("err = {} , so throwing error\n".format(err)));
				throw err;
			}
			var jsonObj = JSON.parse(data);
			if (jsonObj.hasOwnProperty("FileList")) {
				var fileList = jsonObj["FileList"];
				process.stdout.write("=========================================================\n".log);
				process.stdout.write("=========== Right before starting the SENDER! ===========\n".log);
				process.stdout.write("============= There are {} files to send! ===============\n".format(fileList.length).log);
				process.stdout.write("=========================================================\n".log);

				// Create the sender/client/request
				// Print out the headers for each part of the form
				// process.stdout.write(util.inspect(options.headers) + "\n");
				var sender = http.request(options, responseHandler);

				var outputStream = fs.createWriteStream("./formFile.txt");
				var maxUploadSize = uploadSize * Math.pow(2, 10); // Convert size to Kb
				var startByte = 0;
				var endByte = 0;
				var updateStack = [];
				for (var i = 0; i < fileList.length; i++) {
				// Replaced fileList.length with constant 6, to have only 5 files to transfer so I can easily debug; uncomment line above and comment line below for normal run
				// for (var i = 0; i < 2; i++) {
					var chunkNo = 1;
					// process.stdout.write(util.inspect(fileList[i].FileName) + "\n");
					var jsonMetaString = JSON.stringify(fileList[i]);
					// Get the path to the file about to be transfered
					var sourceFilePath = fileList[i]["FilePath"];
					process.stdout.write(sourceFilePath + "\n");
					var sourceFileExt = path.extname(path.basename(sourceFilePath))
					var sourceFileName = path.basename(sourceFilePath, sourceFileExt); // file name without extension to create chunk names
					// process.stdout.write(sourceFileName + "\n");
					// var fileStats = fs.statSync(sourceFilePath);
					// process.stdout.write("And the stats are " + util.inspect(fileStats) + "\n\n" + fileStats.size/maxUploadSize + "\n");
					// form.append("METADATA", jsonMetaString);
					// process.stdout.write(jsonMetaString + "\n");
					// process.stdout.write(util.inspect(fileList[i]) + "\n");
					if (fileList[i]["FileSize"] > maxUploadSize) {
						process.stdout.write(colors.verbose("File is going to be chunked!!!!\n"));
						endByte = 0;
						fileList[i]["TotalChunks"] = Math.ceil(fileList[i]["FileSize"] / maxUploadSize);
						while (endByte < fileList[i]["FileSize"]) {
							var sourceChunkName = sourceFileName + "_" + chunkNo + sourceFileExt;
							startByte = (chunkNo - 1) * maxUploadSize;
							endByte = startByte + maxUploadSize - 1;
							// process.stdout.write(colors.warn("<<<<<<<<<<<<<<<<<<<<< sourceChunkName = " + sourceChunkName + "\n"));
							updateStack.push(updateChunkyForm(form, fileList[i], sourceFilePath, startByte, endByte, chunkNo, sourceChunkName));
								// .spread(writeFormHeader)
								// .fail(function (err) {
								// 	process.stdout.write("------------------ ERROR OCCURED ----- " + err + "\n");
								// });
							chunkNo += 1;
						}
					} else {
						process.stdout.write("File is going to be sent as a whole!!!\n".log);
						jsonMetaString = JSON.stringify(fileList[i]);
						form.append("METADATA", jsonMetaString);
						form.append("FileContent", fs.createReadStream(sourceFilePath));

					}
				}
				form.pipe(outputStream);
				Q.all(updateStack).then(function() {
					process.stdout.write("\n\n+++++++++++++++++++++++++ ALL STACK PROMISES DUE TO CHUNKING FULFILLED +++++++++++++++++++++++++++++++++\n\n".log);
					// process.stdout.write(util.inspect(form) + "\n\n");
					try {
						process.stdout.write("\n\n-------------- Reached the PIPING zone!!! -----------------------------------\n\n".verbose);
						// form.pipe(outputStream);
						form.pipe(sender);
					} catch (err) {
						process.stdout.write("\n\n WTF kind of error is this? ----- {}\n\n\n".format(err).error);
						return;
					}
				});

				// form.pipe(sender); // this automatically calls sender.end and end the request
			} else {
				process.stdout.write("The JSON object does have the property you are looking for!\n".error);
			}
		});
	} catch (err) {
		process.stdout.write("Caught error trying to read and send --->>> {}\n".format(err).error);
	}
} // END OF THE startClient exported function










function responseHandler(res) {
	process.stdout.write(colors.verbose("******************************************************\n"));
	process.stdout.write(colors.verbose("********* Inside the CLIENT callback!!! **************\n"));
	process.stdout.write(colors.verbose("******************************************************\n"));
	process.stdout.write(colors.verbose("******** I got a response from the server! ***********\n"));
	process.stdout.write(colors.info("****** Status code: {} ***********\n".format(res.statusCode)));
	process.stdout.write(colors.verbose("******************************************************\n"));
	process.stdout.write(colors.info("*** Response content type: {} ****\n".format(util.inspect(res.headers["content-type"]))));
	res.setEncoding("utf8");
	if (res.statusCode < 399) {
		var txt = "";
		res.on("data", function(chunk) {
			txt += chunk;
			if (res.statusCode === 200) {
				process.stdout.write(colors.verbose("Message from server: \t") + colors.info(chunk));
			}
		});
		res.on("end", function(data) {
			process.stdout.write(colors.info("****** SERVER HAS FINISHED WITH THE TRANSFER! *****\n"));
		});
	} else {
		var txt = "";
		res.on("data", function(chunk) {
			txt += chunk;
			switch (res.statusCode) {
				case 406:
					process.stdout.write(colors.error("Fire in the hole!!!! >>>>> {}\n".format(http.STATUS_CODES[res.statusCode])));
					process.stdout.write(colors.verbose("Server sends its regards, but with an error code, saying ") + colors.error(http.STATUS_CODES[res.statusCode]) + "\n");
					process.stdout.write(colors.verbose("{}\n".format(chunk)));
					//break;
				};
		});
		res.on("end", function(data) {
			process.stdout.write("<<<<<<< SOMETHING WENT EXTREMELY WRONG WITH THE TRANSFER >>>>>>> {}\n".format(data));
		});
	};
}

// This promise calculates the MD5 for chunks of a bigger file, in order to be able to validate the chunk on the receiver side
// Moreover, the promise updates the chunk headers and content in the form that will later be sent (after all chunks have been processed
var updateChunkyForm = function (form, fileList, filePath, startByte, endByte, chunkNo, sourceChunkName) {
	var hashMD5;
	var deferred = Q.defer();
	var chunkMD5 = crypto.createHash("md5");
	var chunkStream = fs.createReadStream(filePath, {start: startByte, end: endByte});
	chunkStream.on("data", function(d) {
		chunkMD5.update(d);
	});
	chunkStream.on("end", function() {
		hashMD5 = chunkMD5.digest("hex");
		// process.stdout.write(colors.warn("<<<<<<<<<<<<<<<<<<<<< Chunked file: " + filePath + " >>>>>>>>>>>>>>>>>>\n"));
		fileList["StartByte"] = startByte;
		fileList["EndByte"] = endByte;
		fileList["ChunkNumber"] = chunkNo;
		fileList["ChunkName"] = sourceChunkName;
		fileList["ChunkMD5"] = hashMD5;
		// process.stdout.write(util.inspect(fileList) + "\n");
		jsonMetaString = JSON.stringify(fileList);
		form.append("METADATA", jsonMetaString);
		form.append("FileContent", fs.createReadStream(filePath, {start: startByte, end: endByte}));
		deferred.resolve();
	});
	return deferred.promise;
}

module.exports.startClient = startClient;
