var request = require('request');
var colors = require('colors');
var fs = require('fs');

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


var request = require('request');
// request('http://127.0.0.1:1337', function (error, response, body) {
//   // if (!error && response.statusCode == 200) {
//     console.log(body.info);
//     console.log(colors.warn(response.statusCode));
//   // }
// })


// var format = require('string-format');
// format.extend(String.prototype)
// // console.log(format("hello, {}", "thom"));
// console.log("hello, {}".format("Thom"));


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


var path = "/Users/will202";
var new_path = "Programming/arm/sts/test";
buildPath(path, new_path, function(result) {
	console.log(result);
});
