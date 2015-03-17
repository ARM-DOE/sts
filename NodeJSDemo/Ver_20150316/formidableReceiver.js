#!/usr/bin/env sh
':' //; exec "$(command -v nodejs || command -v node)" "$0" "$@"

// HTTP server
// Name:
//		file_transfer_http_server - starts the server waits for client to send files
// Synopsis:
//		file_transfer_http_server
// Author: Laurentiu Dan Marinovici
// Pacific Northwest National Laboratory, Richland, WA
// Last working update: 2015-03-16
//  Changes:
//		- I have eliminated most of the control printouts to make code easier to read.
//		- Moving files from the temporary location to the folder tree similar to the sender side is done using fs.rename
//		- MD5 validation beofre moving is done by looking up the name.


var http = require("http");
var stream = require("stream");
var path = require("path");
var fs = require("fs");
var util = require("util");
var formidable = require("formidable");
var uploadedBytes = 0;

// To create the same folder structure as on the client, the server will eliminate the path up to pathLimName from the file path,
// and create the folder tree in the current server location
var pathLimName = "ARM_project";
var currFolder = process.cwd();

// Following line clear the treminal window and places the cursor at position (0, 0)
process.stdout.write("\u001B[2J\u001B[0;0f");

// Creating an output file to write results in rather than at the terminal
var results = "/receiverResults.txt";
var resultsPath = __dirname + results;
// process.stdout.write(resultsPath + "\t" + fs.existsSync(resultsPath));
if (fs.existsSync(resultsPath)) {
	// If the results file already exists, delete it to have it wiped
	process.stdout.write("\n==== RESULTS FILE EXISTS SO I AM DELETING IT!!!! =====\n");
	fs.unlink(resultsPath);
	// fs.open(resultsPath, "w+");
};

var server = http.createServer();

server
	.on("request", function(req, res) {
		if (req.url === "/upload" && req.method.toLowerCase() === "post") {
			var form = new formidable.IncomingForm();
			form.keepExtensions = true;
			form.uploadDir = "./TempTransfer";
			form.hash = "md5";
			form.multiples = true;
			var files = [];
			var fields = [];
			var filesBegin = [];
			if (!fs.existsSync(path.join(__dirname, form.uploadDir))) {
				process.stdout.write("==== Temporary folder does not exist. Creating it now! ====\n");
				// fs.mkdir(path.join(__dirname, form.uploadDir), mkdirErrCB);
				fs.mkdirSync(path.join(__dirname, form.uploadDir));
			}
			else
			{
				process.stdout.write("==== Temporary folder already exists! ====\n");
			};
		
			form.on("file", function(fileName, fileContent) {
				files.push([fileName, fileContent]);
			});
		
			form.on("fileBegin", function(fileBName, fileBContent) {
				filesBegin.push([fileBName, fileBContent]);
			});
		
			form.on("field", function(fieldName, fieldValue) {
				if (fieldName === "METADATA") {
					var jsonMETADATA = JSON.parse(fieldValue);
				};
				fields.push(jsonMETADATA); // since we only have on field (METADATA) I save only the value of the field, ignoring the name
										   // such that I can easily sort through each element properties
			});

			form.on("end", function() {
				try {
					for (var i = 0; i < this.openedFiles.length; i++) {
						var j = 0;
						while (fields[j]["FileName"] !== this.openedFiles[i].name) {
							j++;
						};
					
						fs.appendFile(resultsPath, "========= Name on sender side (from fields) ====\n");
						fs.appendFile(resultsPath, fields[j]["FileName"]+ "\n"); // file name in the send request fields
						fs.appendFile(resultsPath, fields[j]["FilePath"]+ "\n"); // file path in the send request fields; this would be the path on the sender site
						fs.appendFile(resultsPath, "================ MD5s =========================\n");
						fs.appendFile(resultsPath, "sender   MD5 = " + fields[j]["MD5"] + " =====\n");
						fs.appendFile(resultsPath, "receiver MD5 = " + this.openedFiles[i].hash + " =====\n");
						fs.appendFile(resultsPath,  "==================*********************====================\n");
					
						// Extracting the part needed to preserve the folder tree from the sender on the receiver location
						var indNoTreeRoot = fields[j]["FilePath"].match(pathLimName); // Find the limit pattern in the requested file path; and eliminate everything before that
						var foldTreeRoot = fields[j]["FilePath"].substring(indNoTreeRoot.index + pathLimName.length + 1, path.dirname(fields[j]["FilePath"]).length);
						// Creating the local directory tree
						makeRecDirs(__dirname, foldTreeRoot);
					
						if (this.openedFiles[i].hash === fields[j]["MD5"]) {
							var destFilePath = path.join(__dirname, foldTreeRoot, this.openedFiles[i].name)
							fs.rename(this.openedFiles[i].path, destFilePath, function(renErr) {
								if (renErr) {
									process.stdout.write("ERROR during renaming process!!!\n" + "error message: " + renErr.message + "\n");
									return;
								};
							});
						} else {
							process.stdout.write("ERROR!!!!! MD5's do not match\n");
						};
					};
				} catch(err) {
					process.stdout.write(err + " === Error during validation and moving stages! Transfer should have been done! ===\n");
				};
			});
		
			form.parse(req, function(err, _fields, _files) {
				if (err) {
					console.error(err.message);
					return;
				};
				res.writeHead(200, {"content-type": "text/plain"});
				res.write("RECEIVED UPLOAD.......\n\n");
				res.end("THE END\n");
			});
			/*
			req.on("data", function(data) {
				process.stdout.write("=========************************=======================\n");
				process.stdout.write("req.on -->> " + util.inspect(req.headers) + "\n");
				process.stdout.write("=========************************=======================\n");
			});
			*/
			/*
			form.on("progress", function(bytesReceived, bytesExpected) {
				var percComplete = (bytesReceived / bytesExpected) * 100;
				process.stdout.write("=========== TRANSFER STATISTICS ==========\n");
				process.stdout.write("bytesReceived " + bytesReceived + "===============\n");
				process.stdout.write("bytesExpected " + bytesExpected + "===============\n");
				process.stdout.write("percComplete " + percComplete + "===============\n");
				process.stdout.write("================================================\n");
			});
			*/
		return; // return from the IF statement
		}; // end of IF statement
		
		res.writeHead(200, {"content-type": "text/html"});
		res.end(
			'<form action="upload" enctype="multipart/form-data" method="post">' +
			'<input type="text" name="title"><br>' +
			'<input type="file" name="upload" multiple="multiple"><br>' +
			'<input type="submit" value="Upload">' +
			'</form>'
		);
	}) // end of server ON REQUEST event
	.on("listening", function() {
		var address = server.address();
		process.stdout.write("Server is listening on port " + address.port + "\n");
	})
	.listen(8000, "127.0.0.1", function() {
		var address = server.address();
		process.stdout.write("This HTTP server " + address.address + " is designed to listen on port " + address.port + "\n");
	});
	
function makeRecDirs(curPath, newFoldTree) {
	var splitFoldTree = newFoldTree.split(path.sep);
	// process.stdout.write(curPath + "\n");
	var newPath = curPath + "/" +  splitFoldTree[0];
	if (splitFoldTree[0].length > 0) {
		var existsSync = fs.existsSync(newPath);
		if (existsSync) {
			makeRecDirs(newPath, newFoldTree.substring(splitFoldTree[0].length + 1, newFoldTree.length));
		}
		else {
			fs.mkdirSync(newPath);
			process.stdout.write("Folder " + newPath + " has been created!\n");
			makeRecDirs(newPath, newFoldTree.substring(splitFoldTree[0].length + 1, newFoldTree.length));
		};
	}
	else {
		// process.stdout.write("Reached the bottom of the tree!!!\n");
	}
};

/*
function copyFile(source, destination) {
	
	var inStream = fs.createReadStream(source);
	inStream.on("error", function(err) {
		if (err) {
			process.stdout.write("ERROR creating source stream! Code: " + err + ". Throwing error now! Message: " + err.message + "\n");
			throw err;
		};
	});
	
	var outStream = fs.createWriteStream(destination);
	outStream.on("error", function(err) {
		if (err) {
			process.stdout.write("ERROR creating target stream! Code: " + err + ". Throwing error now! Message: " + err.message + "\n");
			throw err;
		};
	});
	
	inStream.pipe(outStream, {end: false});
	
	inStream.on("end", function() {
		// process.stdout.write("I've just copied the file to " + destination + ", so I am deleting " + source + ".\n");
		fs.unlink(source);
	});
};

var mkdirErrCB = function(err) {
	if (err) {
		process.stdout.write("mkdir ERROR: " + err.message +"\n");
		return;
	}
	else
	{
		process.stdout.write("Succesfully ran MKDIR command!\n");
	};
};
*/
