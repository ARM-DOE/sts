#!/usr/bin/env sh
':' //; exec "$(command -v nodejs || command -v node)" "$0" "$@"

// HTTP server
// Name:
//		file_transfer_http_server - starts the server waits for client to send files
// Synopsis:
//		file_transfer_http_server
// Author: Laurentiu Dan Marinovici
// Pacific Northwest National Laboratory, Richland, WA
// Last update: 2015-02-12

var http = require("http");
var stream = require("stream");
var path = require("path");
var fs = require("fs");
var util = require("util");
var formidable = require("/usr/local/lib/node_modules/formidable");
var uploadedBytes = 0;

// To create the same folder structure as on the client, the server will eliminate the path up to pathLimName from the file path,
// and create the folder tree in the current server location
var pathLimName = "ARM_project";
var currFolder = process.cwd();

// Following line clear the treminal window and places the cursor at position (0, 0)
process.stdout.write("\u001B[2J\u001B[0;0f");

var server = http.createServer();

var fileName = "./test.b";


server
	.on("request", function(req, res) {
		var form = new formidable.IncomingForm();
		form.keepExtensions = true;
		form.uploadDir = "./TempTransfer";
		form.hash = "md5";
		form.multiples = true;
		var files = [];
		var fields = [];
		
		form.on("file", function(name, file) {
			process.stdout.write("------>>>>>>> FORM.ON.FILE <<<<<<<<<<----------\n");
			process.stdout.write(name + "\n");
			process.stdout.write(file.name + "\n");
			process.stdout.write(file.path + "\n");
			process.stdout.write(file.size + "\n");
			process.stdout.write(file.lastModifiedDate + "\n");
			process.stdout.write(file.hash + "\n");
			files.push([name, file]);
			//var receivedStream = fs.createReadStream(file.path);
			//var rndStr = Math.random().toString(16);
			//process.stdout.write(rndStr + "\n");
			//var fileStream = fs.createWriteStream(file.name + rndStr);
			//receivedStream.pipe(fileStream);
		});
		
		form.on("field", function(field, value) {
			process.stdout.write("------>>>>>>> FORM.ON.FIELDS <<<<<<<<<<----------\n");
			process.stdout.write(field + "=" + value + "\n");
			fields.push([field, value]);
		});
		
		/*
		form.on("progress", function(bytesReceived, bytesExpected) {
			process.stdout.write("I've received " + bytesReceived + " bytes!!!!\n");
			process.stdout.write("I am expecting " + bytesExpected + " bytes!!!!\n");
		});
		*/
		form.parse(req, function(err, fields, files) {
			if (err) {
				console.error(err.message);
				return;
			   };

			res.writeHead(200, {'content-type': 'text/plain'});
			process.stdout.write("------>>>>>> FORM.PARSE <<<<<<------\n");   
        	res.write('received upload:\n\n');
        	//process.stdout.write(util.inspect({fields: fields}));
        	//process.stdout.write(util.inspect({files: files}));
        	//res.end();
		});
		
		form.on("end", function() {
			console.log("---->>>> FORM.ON.END ------>>>>>> DONE\n");
			/*
			try {
				res.write("received fields:\n\n" + util.inspect(fields) + "\n");
				res.write("\n\n");
				process.stdout.write("received fields:\n\n" + util.inspect(fields) + "\n");
				process.stdout.write("\n\n");
			} catch(err) {
				res.write(err + "error when receiving fields ????????\n");
				process.stdout.write(err + "error when receiving fields ????????\n");
			};
			*/
			try {
				//res.write("received files:\n\n" + util.inspect(files) + "\n");
				process.stdout.write("received files:\n\n" + util.inspect(files) + "\n");
			} catch(err) {
				//res.write(err + "error when receiving files ?????????\n");
				process.stdout.write(err + "error when receiving files ?????????\n");
			};
			
			//process.stdout.write("------->>>>>>> TEST POSITION IN FORM.ON.END <<<<<<--------\n");
			res.end();
		});
		//return;
		/*/home/laurentiu/work/ARM_project/File_samples/collection/C1/mw
		res.writeHead(200, {'content-type': 'text/html'});
    	res.end(
		  '<form action="/upload" enctype="multipart/form-data" method="post">'+
		  '<input type="text" name="title"><br>'+
		  '<input type="file" name="upload" multiple="multiple"><br>'+
		  '<input type="submit" value="Upload">'+
		  '</form>'
		);
		*/
	})
	.on("listening", function() {
		var address = server.address();
		process.stdout.write("Server is listening on port " + address.port + "\n");
	})
	.listen(8000, "127.0.0.1", function() {
		var address = server.address();
		process.stdout.write("This HTTP server " + address.address + " is designed to listen on port " + address.port + "\n");
	});
