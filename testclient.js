var request = require('request');
var colors = require('colors');

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


var format = require('string-format');
format.extend(String.prototype)
// console.log(format("hello, {}", "thom"));
console.log("hello, {}".format("Thom"));
