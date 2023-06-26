(async function (global) {
    var Openings = {};

    // https://stackoverflow.com/questions/7431268/how-to-read-data-from-csv-file-using-javascript
    function processCSV(allText) {
        var allTextLines = allText.split(/\r\n|\n/);
        var headers = allTextLines[0].split(',');
        var lines = [];

        for (var i = 1; i < allTextLines.length; i++) {
            var data = allTextLines[i].split(',');
            if (data.length == headers.length) {

                var tarr = [];
                for (var j = 0; j < headers.length; j++) {
                    tarr.push(data[j]);
                }
                lines.push(tarr);
            }
        }
        return lines
    }

    // fetch Opening pgn data
    Openings.fetchOpenings = async function () {
        try {
            const response = await fetch('./openings');
            if (response.ok) {
                text = await response.text();
                return processCSV(text)
            } else {
                throw new Error('Error fetching openings CSV');
            }
        } catch (error) {
            console.error('Error:', error);
        }
    };

    global.Openings = Openings;
})(window);