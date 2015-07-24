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
var crypto = require("crypto");
var Q = require("q");
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

var uploadedBytes = 0;

// To create the same folder structure as on the client, the server will eliminate the path up to pathLimName from the file path,
// and create the folder tree in the current server location
var pathLimName = "ARM_project";
var currFolder = process.cwd();

// Following line clear the treminal window and places the cursor at position (0, 0)
process.stdout.write("\u001B[2J\u001B[0;0f");

// Creating an output file to write results in rather than at the terminal
var results = "receiverResults.txt";
var resultsPath = "{}/{}".format(__dirname, results);

if (fs.existsSync(resultsPath)) {
	// If the results file already exists, delete it to have it wiped
	process.stdout.write("\n==== RESULTS FILE EXISTS SO I AM DELETING IT!!!! =====\n");
	fs.unlink(resultsPath);
};

var server = http.createServer();

server.on("listening", function() {
	var address = server.address();
	process.stdout.write("Server is listening on port {}\n".format(address.port));
})
.listen(8000, "127.0.0.1", function() {
	var address = server.address();
	process.stdout.write("This HTTP server {} is designed to listen on port {}\n".format(address.address, address.port));
})
.on("request", function(req, res) {
	// // This code isn't doing anything
	// req.on("data", function(data) {
	// 	// process.stdout.write("=========************************=======================\n");
	// 	// process.stdout.write("req.on -->> " + data.length + "\n");
	// 	// process.stdout.write("=========************************=======================\n");
	// });
	// // Not actually getting to this code
	// req.on("chunk", function(){
	// 	process.stdout.write("========================================== CHUNK ======================================");
	// });

	process.stdout.write("=================== Incoming Request Headers ===================\n");
	process.stdout.write(util.inspect(req.headers) + "\n");
	process.stdout.write("================================================================\n");

	if (req.url === "/upload" && req.method.toLowerCase() === "post") {
		var form = new formidable.IncomingForm();
		form.keepExtensions = true;
		form.uploadDir = "./TempTransfer";
		form.hash = "md5";
		form.multiples = true;

		var files = [];
		var fields = [];
		var filesBegin = [];
		var curJSONmetadata; // this holds the current METADATA at each .on("field"...) call

		if (!fs.existsSync(path.join(__dirname, form.uploadDir))) {
			// This should be replaced with async version
			// Can use buildPath function below
			process.stdout.write("==== Temporary folder does not exist. Creating it now! ====\n");
			// fs.mkdir(path.join(__dirname, form.uploadDir), mkdirErrCB);
			fs.mkdirSync(path.join(__dirname, form.uploadDir));
		} else {
			process.stdout.write("==== Temporary folder already exists! ====\n");
		}

		try {
			// ========================== FORM error EVENT =======================================================
			form.on("error", function(err_message) {
				if (err_message) {
					res.statusCode = 406;
					res.write("There's something wrong with my code\t{}\n".format(err_message));
					res.end("<<<<<<<<<< ENDING FROM INSIDE ERROR EVENT!!! >>>>>>>>>>>>>>\n");
				} else {
					res.write("No error\t" + err_message + "\n");
				}
			})
			// ==================================================================================================

			// ========================== FORM file EVENT =======================================================
			// push the file name and content onto the files array
			.on("file", function(fileName, fileContent) {
				try {
					// process.stdout.write("\n\n***** Inside form.on.FILe *** " + fileName + " AND " + fileContent + "\n\n");
					files.push([fileName, fileContent]);
					// process.stdout.write("\n\n***** Inside form.on.FILe *** " + files + "\n\n");
					var log_message = "************************************* form.on(__file__) *****************************************\n";
					log_message += "+++++++++++++++++++++++++++++++++++++ fileName is ++++++++++++++++++++++++++++++++++++++\n{}\n".format(fileName);
					log_message += "-----------------------------------------------------------------------------------------------------------------\n\n";
					fs.appendFile(resultsPath, log_message, function(err) {
						if (err) {
							process.stdout.write(colors.error("Error while appending to results file in the on.file event!!! Error message -->> {}\n".format(err)));
							throw err;
						}
					});
				} catch (err) {
					process.stdout.write("{} ** form.on.FILE ** ==== Error !!!!! ====\n".format(err));
					return;
				}
			})
			// ==================================================================================================

			// ========================== FORM fileBegin EVENT =======================================================
			// this is where the temporary files or chunks are renamed to correspond to the original file
			// Add the path to the chunk name and add the chunk field and file info to the filesBegin array
			.on("fileBegin", function(fieldB, fileB) {
				try {
					process.stdout.write(colors.formFileBegin("=========================== Inside form.on(__fileBegin__) ===========================\n"));

					if (curJSONmetadata.hasOwnProperty("ChunkName")) {
						process.stdout.write(colors.formFileBegin("Incoming chunk number {} out of {} chunks in total.\n".format(curJSONmetadata["ChunkNumber"], curJSONmetadata["TotalChunks"])));
						fileB.path = path.join(path.dirname(fileB.path), curJSONmetadata["ChunkName"]);
					} else {
						process.stdout.write(colors.formFileBegin("File comes as a whole!!!!!\n"));
						fileB.path = path.join(path.dirname(fileB.path), curJSONmetadata["FileName"]);
					}

					process.stdout.write(colors.formFileBegin("file name: {name}\ntransfer temporarily renamed to file path : {path}\n".format(fileB)));
					process.stdout.write(colors.formFileBegin("=====================================================================================\n"));
					filesBegin.push([fieldB, fileB]);
				} catch (err) {
					process.stdout.write(colors.formFileBegin("{} ** form.on.FileBegin ** ==== Error !!!!! ====\n".format(err)));
					return;
				}
			})
			// ==================================================================================================
// START READING HERE
			// ========================== FORM field EVENT =======================================================
			.on("field", function(fieldName, fieldValue) {
				try {
					if (fieldName === "METADATA") {
						// process.stdout.write(colors.verbose(fieldValue) + "\n");
						curJSONmetadata = JSON.parse(fieldValue);
						// process.stdout.write(colors.prompt(curJSONmetadata) + "\n");
					}
					process.stdout.write(colors.formField("************************* Inside form.on(__field__) ***********************\n"));
					process.stdout.write(colors.formField("File name: " + curJSONmetadata["FileName"] + "\n"));
					if (curJSONmetadata.hasOwnProperty("ChunkName")) {
						process.stdout.write(colors.formField("Chunk number:" + curJSONmetadata["ChunkNumber"] + "\n"));
						process.stdout.write(colors.formField("Chunk name: " + curJSONmetadata["ChunkName"] + "\n"));
					}
					process.stdout.write(colors.formField("****************************************************************************\n"));
					fields.push(curJSONmetadata); // since we only have on field (METADATA) I save only the value of the field, ignoring the name
																				// such that I can easily sort through each element properties
					var print_text = "************************* form.on(__field__) *****************************************\n";
					print_text += "+++++++++++++++++++++++++ fieldName is +++++++++++++++++++++\n{}\n".format(fieldName);
					print_text += "========================= fieldValue is ====================\n{}\n".format(fieldValue);
					print_text += "---------------------------------------------------------------------------------------------------------\n\n";

					fs.appendFile(resultsPath, print_text, function(err) {
						if (err) {
							process.stdout.write(colors.error("Error while appending to results file in the on.field event!!! Error message -->> " + err + "\n"));
							throw err;
						}
					});
				} catch (err) {
					process.stdout.write(colors.formField("{} ** form.on.FIELD ** ==== Error !!!!! ====\n".format(err)));
					return;
				}
			})
			// ==================================================================================================

			// ========================== FORM end EVENT =======================================================
			.on("end", function() {
				try {
					var chunkFiles = [];
					var chunkFields = [];
					var promiseStack = [];
					var tempDestFileMD5;
					var tempDestFiles = []; // uniquely holds the paths to the temporary files created by stitching the chunks; added only when reaching the first chunk
					var destFilePaths = [];
					var origFileMD5s =[]; // uniquely holds the MD5s of the original files form which the chunks originate from; added only when reaching the first chunk
					process.stdout.write(colors.formEnd("*********************************************************************\n"));
					process.stdout.write(colors.formEnd("There is a total of (this.openedFiles.length = ) " + this.openedFiles.length + " files and chunks of files!\n"));
					process.stdout.write(colors.formEnd("*********************************************************************\n"));
					for (var i = 0; i < this.openedFiles.length; i++) {
						process.stdout.write(colors.formEnd("i = " + i + "\n"));
						// var j = 0;
						// skip files until the current one is found inside the fields matrix
						// while (fields[j]["FileName"] !== this.openedFiles[i].name) {
							// j++;
						// };
						// process.stdout.write(colors.formEnd("j = " + j + "\n"));
						var print_text = "========= Name on sender side (from fields) ====\n";
						print_text += "{}\n".format(fields[i]["FileName"]);  // file name in the send request fields
						print_text += "{}\n".format(fields[i]["FilePath"]);  // file path in the send request fields; this would be the path on the sender site
						print_text += "================ MD5s =========================\n".format();
						print_text += "sender   MD5 = {} =====\n".format(fields[i]["MD5"]);
						print_text += "sender   ChunkMD5 = {} =====\n".format(fields[i]["ChunkMD5"]);
						print_text += "receiver MD5 = {} =====\n".format(this.openedFiles[i].hash);
						print_text += "==================*********************====================\n".format();

						fs.appendFile(resultsPath, print_text, function(err) {
							if (err) {
								process.stdout.write(colors.error("Error while appending to results file in on.end event!!! Error message -->> " + err + "\n"));
								throw err;
							}
						});

						// Extracting the part needed to preserve the folder tree from the sender on the receiver location
						var indNoTreeRoot = fields[i]["FilePath"].match(pathLimName); // Find the limit pattern in the requested file path; and eliminate everything before that
						var foldTreeRoot = fields[i]["FilePath"].substring(indNoTreeRoot.index + pathLimName.length + 1, path.dirname(fields[i]["FilePath"]).length);
						// Creating the local directory tree
						makeRecDirs(__dirname, foldTreeRoot);

						if (!fields[i].hasOwnProperty("ChunkName")) {
							// if file comes as a whole
							process.stdout.write(colors.formEnd("Processing a single file, checking its MD5, and moving it to correct location if valid!\n"));
							process.stdout.write(colors.formEnd("File processed : " + this.openedFiles[i].name + "\t@\t" + this.openedFiles[i].path + "\n"));
							if (this.openedFiles[i].hash === fields[i]["MD5"]) {
								var destFilePath = path.join(__dirname, foldTreeRoot, this.openedFiles[i].name)
								fs.rename(this.openedFiles[i].path, destFilePath, function(renErr) {
									if (renErr) {
										process.stdout.write(colors.formEnd("fs.rename CALLBACK --- ERROR during renaming process!!!\n" + "error message: " + renErr.message + "\n"));
										return;
									}
									else {
										// res.writeHead(200, {"content-type": "text/plain"});
										res.statusCode = 200;
										// res.write("fs.rename CALLBACK -- Status Code = " + res.statusCode + "\n");
										res.end();
										//res.end("<<<< NO ACTION NEEDED -- THE END >>>>\n");
									}
								});
								// res.writeHead(200, {"content-type": "text/plain"});
								res.statusCode = 200;
								res.write("Status Code = {}. Receiver says file {} has been succesfully transfered.\n".format(res.statusCode, this.openedFiles[i].path));
								res.end();
								// res.end("<<<< NO ACTION NEEDED -- THE END >>>>\n");
							} else {
								process.stdout.write(colors.formEnd("ERROR!!!!! MD5's do not match\n"));
								// res.writeHead(406, {"content-type": "text/plain"});
								// res.statusCode = 406;
								// res.write("Status Code = " + res.statusCode + ". File " + colors.verbose(this.openedFiles[i].path) + " transfer went wrong!!!\n");
								//res.end("<<<< Please, RE-TRANSMIT FILE --- THE END >>>>\n");
								this.emit("error", "FILE ---- THIS IS MAD !!!!!!!!!!!!!!!!!!! ----- CODE " + res.statusCode);
							}
						} else {
							// if file comes in chunks
							// Temporary path to combined file will be in the same location as the chunks
							var tempDestFile = path.join(path.dirname(this.openedFiles[i].path), this.openedFiles[i].name);

							if (tempDestFiles.indexOf(tempDestFile) < 0) {
								tempDestFiles.push(tempDestFile);
								destFilePaths.push(path.join(__dirname, foldTreeRoot, this.openedFiles[i].name));
								origFileMD5s.push(fields[i]["MD5"]);
								process.stdout.write(colors.formEnd("Temp path :" + tempDestFile + "\n"));
							} else {
								process.stdout.write(colors.formEnd("The path for the chunk's parent already exists at: " + tempDestFiles[tempDestFiles.indexOf(tempDestFile)] + ".\n"));
							}

							chunkFiles.push(this.openedFiles[i].path);
							chunkFields.push(fields[i]);
							// If the chunk has been correctly received, based on its hash, read it and then write it to the corresponding position in the output
							// if (validateFileTransfer(this.openedFiles[i].hash, fields[i]["ChunkMD5"])) {
							if (this.openedFiles[i].hash === fields[i]["ChunkMD5"]) {
								process.stdout.write(colors.error("\n************** Iteration number i = " + i + " VALIDATED ***************************\n"));
								process.stdout.write(colors.formEnd("Counted for chunk number " + fields[i]["ChunkNumber"] + "\n"));
								process.stdout.write(colors.formEnd("with the name: " + fields[i]["ChunkName"] + "\n"));
								process.stdout.write(colors.formEnd("Hash from the chunk header: " + fields[i]["ChunkMD5"] + "\n"));
								process.stdout.write(colors.formEnd("Path processed: " + this.openedFiles[i].path + " of file name " + this.openedFiles[i].name + "\n"));
								process.stdout.write(colors.formEnd("Hash for current read file from FORMIDABLE : " + this.openedFiles[i].hash + "\n"));
								process.stdout.write(colors.formEnd("Temporarily, it should end at : " + tempDestFile + "\n"));
								process.stdout.write(colors.error("\n*************************************************************************************\n"));
								// promiseStack.push(chunkRead(this.openedFiles[i].path, fields[i], tempDestFile)
								//  .spread(chunkWrite));
								promiseStack.push(readWriteChunk(this.openedFiles[i].path, fields[i], tempDestFile).then(function(res) {
									// process.stdout.write(colors.info("\tValidated the chunk with result -->> " + res + "\n"));
								}));
							} else {
								process.stdout.write(colors.formEnd("Chunk number " + fields[i]["ChunkNumber"] + " (" + fields[i]["ChunkName"] + ")\n"));
								process.stdout.write(colors.formEnd("of file " + fields[i]["FileName"] + " has not been transferred correctly.\n"));
								process.stdout.write(colors.formEnd("Please, re-transmit!! Need to look into how to do this.\n"));
								// res.writeHead(406, {"content-type": "text/plain"});
								// res.setHeader("Content-Type", "text/plain");
								// res.statusCode = 406;
								// res.write("Status Code = " + res.statusCode + ". Chunk " + colors.verbose(fields[i]["ChunkName"]) + " transfer went wrong!!!\n");
								//res.end("<<<< Please, RE-TRANSMIT CHUNK -- THE END >>>>\n");
								this.emit("error", "CHUNK ---- THIS IS CRAZY MAD !!!!!!!!!!!!!!!!!!!! ----- CODE {}".format(res.statusCode));
							}
						}
					}

					if (promiseStack.length > 0) {
						 Q.all(promiseStack).then(function () { // all fulfilled
							 try {
								 process.stdout.write(colors.formEnd("\n\n======================== DONE STITCHING ALL CHUNKS TO RECREATE THE ORIGINALS!!! =====================\n"));
								 process.stdout.write(colors.formEnd("The originals are: " + tempDestFiles + "\n"));
								 process.stdout.write(colors.formEnd("The destination paths are: " + destFilePaths + "\n"));
								 process.stdout.write(colors.formEnd("The original MD5s are: " + origFileMD5s + "\n"));
								 for (iFile in tempDestFiles) {
									var sentMD5 = origFileMD5s[iFile];
									validateAndMove(tempDestFiles[iFile], origFileMD5s[iFile], destFilePaths[iFile]).spread(function (del, toDelete) {
										if (del === true) {
											for (jFile in chunkFiles) {
												if (chunkFields[jFile]["FileName"] === path.basename(toDelete)) {
													process.stdout.write(colors.formEnd("Deleting chunk " + chunkFields[jFile]["ChunkName"] + " corresponding to file " + path.basename(toDelete) + "\n"));
													fs.unlink(chunkFiles[jFile]);
												}
											}
										} else {
											process.stdout.write(colors.error("Not deleting chunks corresponding to " + path.basename(toDelete) + " yet!!!!!\n"));
										}
									});
								 }
							 } catch (err) {
								 process.stdout.write(colors.formEnd(err + " >>>> ERROR DURING THE STITCHED FILE VALIDATION AND MOVE!\n"));
							 }
						 });
					} else {
						process.stdout.write("No need for PROMISES? That means, no chunked files, or something might have been wrong with some chunks.\n");
					}
					// res.writeHead(200, {"content-type": "text/plain"});
					// res.write("RECEIVED UPLOAD.......\n\n");
					//res.setHeader("Content-Type", "text/plain");
					res.end("THE END, this is my blood you drink!!!!\n");
					server.close();
				} catch(err) {
					process.stdout.write(colors.formEnd(err + " ** form.on.END ** === Error during validation and moving stages!\nTransfer to temporary files should have been done, though! ===\n"));
				}
			}) // End the form.on(end)
			// ==================================================================================================

			// ========================== FORM parse API =======================================================
			.parse(req, function(err, _fields, _files) {
				if (err) {
					console.error(colors.error("Error thrown from PARSE API ---- \t" + err + "\tmaxFieldsSize = " + form.maxFieldsSize + "\n")); // + util.inspect(_fields) + "\n" + util.inspect(_files)));
					process.exit(1);
				}
				// process.stdout.write("\n\n================*********** Form PARSE ****************\n\n");
				// process.stdout.write(util.inspect(_fields) + "\n");
				// process.stdout.write(_files.name + "\n");
			});
			// ==================================================================================================

		} catch (err) {
			process.stdout.write(colors.formEnd("I think I just caught this error!?!?!\t" + err + "\n"));
		}

		return; // return from the IF statement
	} // end of IF statement

 // server.close();
}); // end of server ON REQUEST event

/********************************************************************************
 ******************************      Functions     ******************************
 ********************************************************************************/

/*
 * Create the given directory structure
 */
function makeRecDirs(curPath, newFoldTree) {
	var splitFoldTree = newFoldTree.split(path.sep);
	var newPath = "{}/{}".format(curPath, splitFoldTree[0]);

	if (splitFoldTree[0].length > 0) {
		var existsSync = fs.existsSync(newPath);
		if (existsSync) {
			makeRecDirs(newPath, splitFoldTree.slice(1).join(path.sep));
			// makeRecDirs(newPath, newFoldTree.substring(splitFoldTree[0].length + 1, newFoldTree.length));
		} else {
			fs.mkdirSync(newPath);
			process.stdout.write("Folder " + newPath + " has been created!\n");
			makeRecDirs(newPath, splitFoldTree.slice(1).join(path.sep));
			// makeRecDirs(newPath, newFoldTree.substring(splitFoldTree[0].length + 1, newFoldTree.length));
		}
	}
}

function buildPath(path, tree, callback) {
	// this function should eventually replace the above makeRecDirs
	// fs.existsSync is being deprecated by node, and this function works asyncronously
	path_array = path.split('/');
	split_path = tree.split('/');

	if (split_path[0].length > 0) {
		path_array.push(split_path[0]);
		new_path = path_array.join('/');

		fs.exists(new_path, function(exists) {
			if (exists) {
				console.log("directory exists");
				buildPath(new_path, split_path.slice(1).join('/'), callback)
			} else {
				fs.mkdir(new_path, function() {
					console.log("created directory");
					buildPath(new_path, split_path.slice(1).join('/'), callback);
				});
			}
		});
	} else {
		callback(new_path);
	}
}

/*
 * Reading each chunk as it arrives into a buffer to be then stitched to a temporary file representing a temporary copy of the original file on the receiver side
 */
// Not currently used
/*
var chunkRead = function (chunkFile, fields, destFile) {
	var deferred = Q.defer();
	fs.stat(chunkFile, function(err, stats) {
		fs.open(chunkFile, "r", function(err, fdR) {
			if (err) {
				process.stdout.write(colors.error("ERROR opening the chunk: " + err.message + "\n"));
			} else {
				var readBuffer = new Buffer(stats.size);

				fs.read(fdR, readBuffer, 0, readBuffer.length, null, function (err, bytesRead, readBuffer) {
					if (err) {
						process.stdout.write("ERROR while reading the chunk: " + chunkFile + "\n");
					} else {
						process.stdout.write("Chunk opened for reading: " + chunkFile + "\n");
						process.stdout.write("Reading chunk: " + chunkFile + "\n");
						process.stdout.write("Size of the buffer: " + readBuffer.length + "\n");
						process.stdout.write("Reading chunk for writing successful!\n");
						fs.close(fdR, function(err) {
							if (err) {
								deferred.reject(new Error(err));
								process.stdout.write(colors.error("ERROR while trying to close the chunk part!\n"));
							} else {
								deferred.resolve([readBuffer, fields, destFile]);
							}
						});
					}
				});
			}
		});
	});

	return deferred.promise;
}*/

/*
 * Write the chunks to their correct location within the destination file
 */
// Not currently used
/*
var chunkWrite = function (readBuffer, chunkFields, tempDestFile) {
	var deferred = Q.defer();
	fs.open(tempDestFile, "w", function(err, fdW) {
		if (err) {
			process.stdout.write("ERROR opening the temporary destination file! {}\n".format(err.message));
		} else {
			var bufferWrite = new Buffer(readBuffer);
			fs.write(fdW, bufferWrite, 0, bufferWrite.length, chunkFields["StartByte"], function (err, bytesWritten, bufferWrite) {
				if (err) {
					process.stdout.write(colors.error("ERROR while writing chunk from byte {}\n".format(chunkFields["StartByte"])));
				} else {
					process.stdout.write("\n=== Just called chunkWrite with: =====\nWriting {}\nto {}\n".format(chunkFields["ChunkName"], tempDestFile));
					process.stdout.write("starting at StartByte = {} with bufferSize = {}\n".format(chunkFields["StartByte"], bufferWrite.length));
					process.stdout.write(colors.warn("BytesWritten = ", bytesWritten));
					process.stdout.write("End Byte = {}\n".format(chunkFields["EndByte"]));

					fs.close(fdW, function(err) {
						if (err) {
							deferred.reject(new Error(err));
							process.stdout.write("ERROR while trying to close the merged file after writing chunk {}\n".format(chunkFile));
						} else {
							deferred.resolve(tempDestFile);
						}
					});
				}
			});
		}
	});
	return deferred.promise;
}
*/
/*
 * Validate transferred file or chunk
 */
var validateFileTransfer = function (receiverHash, senderHash) {
	return (receiverHash === senderHash) ? true : false;
}

/*
 * Validate the stitched file
 * Move it to the right location if validated
 */
var validateAndMove = function (filePath, origMD5, destPath) {
	var deferred = Q.defer();
	var hashMD5;
	var chunkMD5 = crypto.createHash("md5");
	var chunkStream = fs.ReadStream("{}/{}".format(__dirname, filePath));

	chunkStream.on("data", function(d) {
		chunkMD5.update(d);
	})
	.on("error", function(err) {
		process.stdout.write(colors.error("ERROR in validateAndMove!!!! --- {}\n".format(err)));
	})
	.on("end", function() {
		hashMD5 = chunkMD5.digest("hex");
		process.stdout.write(colors.info("==================== TIME TO VALIDATE THE FRANKENSTEIN FILE {} AND COPY IT TO RIGHT LOCATION IF OKAY! ========\n".format(filePath)));
		process.stdout.write(colors.error("Original MD5:   {}\n".format(origMD5)));
		process.stdout.write(colors.error("Calculated MD5: {}\n".format(hashMD5)));
		if (hashMD5 === origMD5) {
			process.stdout.write("Stitched file is going to be moved to its correct location!\n");
			fs.rename(filePath, destPath, function(renErr) {
				if (renErr) {
					process.stdout.write(colors.formEnd("ERROR during renaming process for the stiched file!!!\nerror message: {}\n".format(renErr.message)));
					return;
				} else {
					deferred.resolve([true, filePath]);
				}
			});
		} else {
		 process.stdout.write(colors.formEnd("ERROR!!!!! MD5 for the stitched file does not match the original file MD5!\n"));
		 deferred.resolve([false, filePath]);
		}
	});

	return deferred.promise;
}

/*
 * Read the chunk from it's temporary file
 * Write the chunk into it's destination file in the correct location within the file
 */
var readWriteChunk = function (chunkFilePath, chunkFields, destFilePath) {
	var deferred = Q.defer();
	try {
		var read_path = "{}/{}".format(__dirname, chunkFilePath);
		var readStream = fs.createReadStream(path, function(err) {
			if (err) {
				process.stdout.write(colors.error("readWriteChunk ERROR while creating the READ stream. ERROR message -->> {}\n".format(err)));
			}
		});

		var write_path = "{}/{}".format(__dirname, destFilePath;
		var writeStream = fs.createWriteStream(write_path, {flags: "w", start: chunkFields["StartByte"]}, function(err) {
			if (err) {
				process.stdout.write(colors.error("readWriteChunk ERROR while creating the WRITE stream. ERROR message -->> {}\n".format(err)));
			}
		});

		readStream.pipe(writeStream);

		writeStream.on("finish", function() {
			deferred.resolve("readWriteChunk RESOLVED for {} into {} at {}\t".format(chunkFilePath, destFilePath, chunkFields["StartByte"]));
			deferred.resolve();
		});

	} catch (err) {
		process.stdout.write(colors.error("ERRRRORRRRRRR!!!!! Message: {}\n".format(err)));
	}

	return deferred.promise;
}
