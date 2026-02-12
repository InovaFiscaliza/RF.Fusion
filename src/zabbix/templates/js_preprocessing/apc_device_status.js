// Convert APC HTML to Numeric Uptime

// load the HTML content from a file - test only
// var value = require('fs').readFileSync('/root/RF.Fusion/test/zabbix/js_preprocessing/Device Status Summary Page.html', 'utf8');

// Split the value into substrings and put these substrings into an array
var lines = value.split('\n');

// Create list of variables to recover from Measure-Status Page
var parameter = 'UpTime';

// Loop through the "lines" array
for (var i = 0; i < lines.length; i++) {
    // use match() to find the parameter with index parameter_index
    var line = lines[i].match(parameter);

    // If the parameter is found, use match() with a regular expression to find the value in the following line
    if (line !== null) {
        // split the uptime into days, hours and minutes
        var days = lines[i+1].match(/\d{1,4} Days/)[0];
        var hours = lines[i+1].match(/\d{1,2} Hours/)[0];
        var minutes = lines[i+1].match(/\d{1,2} Minutes/)[0];

        // remove the words from the strings
        days = days.replace(' Days', '');
        hours = hours.replace(' Hours', '');
        minutes = minutes.replace(' Minutes', '');

        // convert the strings to numbers
        days = Number(days);
        hours = Number(hours);
        minutes = Number(minutes);

        // calculate the total uptime in seconds
        var uptime = (days * 86400) + (hours * 3600) + (minutes * 60);

        break;
    }
}

// print the output for testing
// console.log(uptime);

return uptime;