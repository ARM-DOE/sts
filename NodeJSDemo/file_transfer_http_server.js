// HTTP server

var http = require("http");
var stream = require("stream");
var path = require("path");
var fs = require("fs");
var uploadedBytes = 0;

// To create the same folder structure as on the client, the server will eliminate the path up to pathLimName from the file path,
// and create the folder tree in the current server location
var pathLimName = "ARM_project";
var currFolder = process.cwd();

var server = http.createServer();

server
	.on("request", function(req, res) { // req= request, used to learn details about the request, res = result, used to write back to client
		var uploadedBytes = 0;
		req.on("data", function(reqBody) {
			var reqFilePath = reqBody.toString();
			/*
			// Extracting the part needed to preserve the folder tree from the client on the server location
			var indNoTreeRoot = reqFilePath.match(pathLimName); // Find the limit pattern in the requested file path; and eliminate everything before that
			process.stdout.write("***** The pattern I've been looking for is " + indNoTreeRoot + " *********\n");
			var foldTreeRoot = reqFilePath.substring(indNoTreeRoot.index + pathLimName.length + 1, path.dirname(reqFilePath).length);
			process.stdout.write("***** I should create this new tree " + path.join(__dirname, foldTreeRoot) + " **********\n");
			*/
			fs.stat(reqFilePath, function(err, fileStats) {
				process.stdout.write("Client just sent this file: \n" + reqBody + "\n");
				var fileBytes = fileStats["size"];

				// Extracting the part needed to preserve the folder tree from the client on the server location
				var indNoTreeRoot = reqFilePath.match(pathLimName); // Find the limit pattern in the requested file path; and eliminate everything before that
				process.stdout.write("***** The pattern I've been looking for is " + indNoTreeRoot + " *********\n");
				var foldTreeRoot = reqFilePath.substring(indNoTreeRoot.index + pathLimName.length + 1, path.dirname(reqFilePath).length);
				process.stdout.write("***** I should create this new tree " + path.join(__dirname, foldTreeRoot) + " **********\n");
				// Creating the local directory tree
				makeRecDirs(__dirname, foldTreeRoot);

				// Create the input stream
				var inputStream = fs.createReadStream(reqBody.toString());
				
				// Creating the destination file from the name of the file in the request and curretn folder
				var destFilePath = path.join(__dirname, foldTreeRoot, path.basename(reqFilePath));
				// Creating the ouput stream
				var outputStream = fs.createWriteStream(destFilePath);

				res.write("Request is for file name " + reqBody); // writing the response body
				res.write("Total number of bytes to be transferred: " + fileBytes);
				// Piping the input stream to the local output
				inputStream.pipe(outputStream, {end: false}); // piping the file from client side onto server side
				
				inputStream.on("data", function(chunk) {
					uploadedBytes += chunk.length; // Track the uploading process
					var progress = (uploadedBytes / fileBytes) * 100;
					res.write("Progress for " + path.basename(reqFilePath) + " : " + parseInt(progress, 10) + "%");
				});
				
				inputStream.on("end", function() { // emitting the end event once all chuncks for each file have been uploaded
					res.write("Transfer for " + path.basename(reqFilePath) + " DONE!");
					res.end();
				});
			});
		});
		res.writeHead(200, {"Content-Type": "text/plain"}); // setting up the response type
	})
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
	// process.stdout.write(splitFoldTree + "\n");
	var newPath = curPath + "/" +  splitFoldTree[0];
	if (splitFoldTree[0].length > 0) {
		var existsSync = fs.existsSync(newPath);
		if (existsSync) {
			// process.stdout.write("Folder " + newPath + " already exists!\n")
			makeRecDirs(newPath, newFoldTree.substring(splitFoldTree[0].length + 1, newFoldTree.length));
		}
		else {
			fs.mkdir(newPath, function(err) {
				if (err) {
					console.error("mkdir error:    " + err.message);
				}
			});
			// process.stdout.write("Folder " + newPath + " has been created!\n");
			makeRecDirs(newPath, newFoldTree.substring(splitFoldTree[0].length + 1, newFoldTree.length));
		};
	}
	else {
		// process.stdout.write("Reached the bottom of the tree!!!\n");
	}
};
