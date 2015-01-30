// HTTP Client

var http = require("http");
var fs = require("fs");
var path = require("path")

var fileListSource = path.join(__dirname, "fileList01.json");

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
			
		for (var i = 0; i < fileList.length - 1; i++) {
			// Get the path to the file about to be transfered
			var sourceFilePath = fileList[i]["FilePath"];
			process.stdout.write(sourceFilePath + "\n");
			client.write(sourceFilePath, "utf8");
		}
		
		client.end();
	}
	else {
		process.stdout.write("The JSON object does have the property you are looking for!\n");
	}
});

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
