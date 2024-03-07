// Convert APC HTML to JSON

// load the HTML content from a file - test only
// var value = require('fs').readFileSync('/root/RF.Fusion/src/zabbix/templates/APC/elsys_status.html', 'utf8');

// Split the value into substrings and put these substrings into an array
var html_input = value.split('\n');

// Create list of variables to recover from UPS Status Page
var parameter_output = {
    'ip_address': "",
    'nivel_sinal': "",
    'nivel_sinal_percent': "",
    'nomeOperadoraNotParsed': "",
    'tecnologia': "",
    'banda': "",
    'freq_3G': "",
    'imei': "",
    'tipo_rede_movel': "",
    'tipo_de_antena': "",
    'levelQuality': "",
    'iccid': "",
    'wan_address': ""
};

var parameter_list = Object.keys(parameter_output);

var parameter_index = 0;
var line = 0;
// Loop through the "html_input" array
while (parameter_index < parameter_list.length) {
    line++;
    
    var key_match = html_input[line].match(parameter_list[parameter_index]);

    // If the parameter is found, use match() with a regular expression to find the value in the following line
    if (key_match !== null) {
        // replace " with ' to avoid problems with parsing
        html_input[line] = html_input[line].replace(/"/g, "'");
        html_input[line] = html_input[line].replace(/''/g, "'");

        // use match() to find the parameter value enclosed by ' in the line
        var parameter_value = html_input[line].match(/'.*'/)[0] || "";

        parameter_output[parameter_list[parameter_index]] = parameter_value.slice(1, -1);
        parameter_index++;
    }
}

var output = JSON.stringify(parameter_output);

// print the output for testing
// console.log(output);

// Return JSON string
return output;