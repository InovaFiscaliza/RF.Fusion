// Convert APC HTML to JSON

// load the HTML content from a file - test only
// var value = require('fs').readFileSync('/root/RF.Fusion/test/zabbix/js_preprocessing/Measure-UPS Status Page.html', 'utf8');

// Split the value into substrings and put these substrings into an array
var lines = value.split('\n');

// Create list of variables to recover from Measure-Status Page
var parameter_output = {
    'Temperature': 0,
    'Humidity': 0,
    'ENERGIA': 'Yes',
    'INCENDIO': 'Yes',
    'INVASAO': 'Yes',	
    'BALIZAMENTO': 'Yes'
};

var parameter_list = Object.keys(parameter_output);

var parameter_index = 0;
// Loop through the "lines" array
for (var i = 0; i < lines.length; i++) {
    // use match() to find the parameter with index parameter_index
    var line = lines[i].match(parameter_list[parameter_index]);

    // If the parameter is found, use match() with a regular expression to find the value in the following line
    if (line !== null) {
        //test if parameter_output is a number or a string
        if (typeof parameter_output[parameter_list[parameter_index]] == 'number'){
            var parameter_match = lines[i+1].match(/>\d{2,3}.{0,1}\d{1,2}</)[0] || ">0<"
            parameter_output[parameter_list[parameter_index]] = Number(parameter_match.slice(1, -1));
        } else {
            var parameter_match = lines[i+1].match(/>Yes<|>No</)[0] || "No"
            if (parameter_match == ">Yes<") {
                parameter_output[parameter_list[parameter_index]] = 1;
            }
            else {
                parameter_output[parameter_list[parameter_index]] = 0;
            }
        }

        parameter_index++;
        if (parameter_index == parameter_list.length) {
            break;
        }
        i = i + 4;
    }
}

var output = JSON.stringify(parameter_output);

// remove spaces from the output
output = output.replace(/ /g, '');

// print the output for testing
// console.log(output);

// Return JSON string
return output;