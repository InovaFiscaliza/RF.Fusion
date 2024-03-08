// Convert APC HTML to JSON

// load the HTML content from a file - test only
//var value = require('fs').readFileSync('/root/RF.Fusion/test/zabbix/js_preprocessing/elsys_status_2.html', 'utf8');

// Split the value into substrings and put these substrings into an array
var html_input = value.split('\n');

// Create list of variables to recover from UPS Status Page
var parameter_output = {
    'ip_address': "ND",	
    'nivel_sinal': "0 dBm",
    'nivel_sinal_percent': "0 %",
    'nomeOperadoraNotParsed': "ND",
    'tecnologia': "ND",
    'banda': "0 MHz",
    'freq_3G': "0 MHz",
    'imei': "ND",
    'tipo_rede_movel': "ND",
    'tipo_de_antena': "ND",
    'levelQuality': "0 dB",
    'iccid': "ND",
    'wan_address': "ND"
};

var parameter_list = Object.keys(parameter_output);

// Loop through the "html_input" array
for (var line = 0; line < html_input.length; line++) {
    
    for (var i = 0; i < parameter_list.length; i++) {
        
        var key_match = html_input[line].match(parameter_list[i]);

        // If the parameter is found, use match() with a regular expression to find the value in the following line
        if (key_match !== null) {
            // replace " with ' to avoid problems with parsing
            html_input[line] = html_input[line].replace(/"/g, "'");
            html_input[line] = html_input[line].replace(/''/g, "'");

            // use match() to find the parameter value enclosed by ' in the line
            var parameter_value = html_input[line].match(/'.*'/)[0] || "";

            parameter_output[parameter_list[i]] = parameter_value.slice(1, -1);
            
            // remove the parameter from the list
            parameter_list.splice(i, 1);

            break;
        }
    }

    if (parameter_list.length == 0) {
        break;
    }
}

var output = JSON.stringify(parameter_output);

// print the output for testing
//console.log(output);

// Return JSON string
return output;