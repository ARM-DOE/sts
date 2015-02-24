#!/usr/bin/env sh
':' //; exec "$(command -v nodejs || command -v node)" "$0" "$@"
// Name:
//		fileWatcher - watches a file/folder for changes
// Synopsis:
//		fileWatcher [option] [file path]
// Available options:
//		--d, -directory
//			to watch a specific folder; find out if files are added, deleted or modified
//		--f, -file
//			to watch a specific file
// Author: Laurentiu Dan Marinovici
// Pacific Northwest National Laboratory, Richland, WA
// Last update: 2015-02-12

var fs = require("fs");
var path = require("path");
var util = require("util");
var colors = require("/usr/local/lib/node_modules/colors");

// Set color theme 
colors.setTheme({
	silly:		'rainbow',
	input:		'grey',
	verbose:	'cyan',
	prompt:		'grey',
	info:		'green',
	data:		'grey',
	help:		'cyan',
	warn:		'yellow',
	debug:		'blue',
	error:		'red'
});

// Following line clear the treminal window and places the cursor at position (0, 0)
process.stdout.write("\u001B[2J\u001B[0;0f");

function watcher() {
	// get the argument list
	var inputArgs = process.argv;
	
	// If there are only 2 arguments, that means that no flags and/or folder/file to be watched have been specified
	if (inputArgs.length <= 2) {
		process.stdout.write("No flags or source have been specified!\nRerun using any of these flags and the correct source folder/file.\n");
		process.stdout.write("Aproved flag list:\n \t--d or --directory\t = watch a folder\n \t--f or --file     \t = watch a file\n");
		process.exit();
	}
	
	// Case of too many arguments. Could this ever happen?
	else if (inputArgs.length > 4) {
		process.stdout.write("Too many input arguments. Check the correct syntax!\n");
		process.exit();
	}
	
	// Correct call should be with 4 arguments in total
	else {
		var i = 2; // Current argument index; indexing starts at 0
		// The first two arguments, indexes 0 and 1, are nodejs and the current file name
		while (i < inputArgs.length) {
			var currArg = inputArgs[i];
			// process.stdout.write("Current argument ---- " + currArg + "\n");

			if (inputArgs[i + 1]) { // If there is a new argument after the flag
				if (currArg === "--d" || currArg === "-directory") {
					watchedFolder = inputArgs[i + 1];
					// process.stdout.write(inputArgs[i + 1]);
					watchedFoldPath = path.resolve(watchedFolder);
					process.stdout.write("Watching folder " + watchedFoldPath + "\n");
					i = i + 2;
					fs.watch(watchedFoldPath, {persinstent: true}, function(event, watchedName) {
						process.stdout.write("EVENT is: " + event + "\n");
						// watchedName is only the name of the folder/file, no matter if it gets specified with the entire path at the terminal, for example 
						modFoldPath = path.resolve(watchedFoldPath, watchedName);
						fs.stat(modFoldPath, function(err, stats) {
							if (err) {
								process.stdout.write(colors.warn("Error in the fs.stat function.\n"));
								if (err.code === "ENOENT") {
									errMsg = "File/folder does not exist. I think you've just moved or deleted it!\n";
									process.stdout.write(colors.warn(errMsg));
									return; // Interrupting the function and returning control back to the main code
								}
								else {
									process.stdout.write("Error code this time: " + err.code + "\n");
									return;
								};
							};
							if (event === "rename") {
								if (stats.isDirectory()) {
									process.stdout.write("Folder " + modFoldPath + " might have been created/renamed/deleted!\n");
								}
								else if (stats.isFile()) {
									process.stdout.write("File " + modFoldPath + " might have been created/renamed/deleted!\n");
								}
							}
							else if (event === "change") {
								if (stats.isDirectory()) {
									process.stdout.write("Folder " + modFoldPath + " might have been changed!\n");
								}
								else if (stats.isFile()) {
									process.stdout.write("File " + modFoldPath + " might have been changed!\n");
								}
							};
						});
					});
				}
				else if (currArg == "--f" || currArg == "-file") {
					watchedFile = inputArgs[i + 1];
					watchedFilePath = path.resolve(watchedFile);
					process.stdout.write("Watching file " + watchedFilePath + "\n");
					i = i + 2;
				}
				else {
					process.stdout.write("Flag " + currArg + " is INCORRECT. Check again and rerun, please!\n");
					process.stdout.write("Aproved flag list:\n\t--d or --directory\t = watch a folder\n\t--f or --file      =\t watch a file\n");
					// i = i + 1;
					// return;
					process.exit();
				};
			}
			else {
				process.stdout.write("No source file/folder has been specified after the flag. Rerun with a correct file/folder path\n");
				// i = i + 2;
				process.exit();
			};
		};
	};
};

watcher();
