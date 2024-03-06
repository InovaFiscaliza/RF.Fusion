// Convert APC HTML to JSON

// load the HTML content from a file - test only
//var value = require('fs').readFileSync('/root/RF.Fusion/src/zabbix/templates/APC/UPS Status Page.html', 'utf8');

// Split the value into substrings and put these substrings into an array
var lines = value.split('\n');

// Create list of variables to recover from UPS Status Page
var parameter_output = {
    'Runtime Remaining': 0,
    'Internal Temperature': 0,
    'Input Voltage': 0,
    'Input Frequency': 0,
    'Maximum Line Voltage': 0,
    'Minimum Line Voltage': 0,
    'Output Voltage': 0,
    'Output Frequency': 0,
    'Load Power': 0,
    'Battery Capacity': 0,
    'Battery Voltage': 0,
    'Batteries': 0
};

var parameter_list = Object.keys(parameter_output);

var parameter_index = 0;
// Loop through the "lines" array
for (var i = 0; i < lines.length; i++) {
    // use match() to find the parameter with index parameter_index
    var line = lines[i].match(parameter_list[parameter_index]);

    // If the parameter is found, use match() with a regular expression to find the value in the following line
    if (line !== null) {
        var parameter_match = lines[i+1].match(/>\d{2,4}.{0,1}\d{0,2}</)[0] || ">0<"
        parameter_output[parameter_list[parameter_index]] = Number(parameter_match.slice(1, -1));
        parameter_index++;
        if (parameter_index == parameter_list.length) {
            break;
        }
        i = i + 5;
    }
}

var output = JSON.stringify(parameter_output);

// remove spaces from the output
output = output.replace(/ /g, '');

// print the output for testing
//console.log(output);

// Return JSON string
return output;