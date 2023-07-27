// Code by Euler Junior on https://stackoverflow.com/a/32257791
(function (global){
    var Colors = {};

    function hex (c) {
      var s = "0123456789abcdef";
      var i = parseInt (c);
      if (i == 0 || isNaN (c))
        return "00";
      i = Math.round (Math.min (Math.max (0, i), 255));
      return s.charAt ((i - i % 16) / 16) + s.charAt (i % 16);
    }

    /* Convert an RGB triplet to a hex string */
    function convertToHex (rgb) {
      return hex(rgb[0]) + hex(rgb[1]) + hex(rgb[2]);
    }

    /* Remove '#' in color hex string */
    function trim (s) { return (s.charAt(0) == '#') ? s.substring(1, 7) : s }

    /* Convert a hex string to an RGB triplet */
    function convertToRGB (hex) {
      var color = [];
      color[0] = parseInt ((trim(hex)).substring (0, 2), 16);
      color[1] = parseInt ((trim(hex)).substring (2, 4), 16);
      color[2] = parseInt ((trim(hex)).substring (4, 6), 16);
      return color;
    }

    Colors.generateColor = function (colorStart,colorEnd,colorCount){
        // Define beginning and end of gradient as well as number of colors to find
        var start = convertToRGB (colorStart);
        var end   = convertToRGB (colorEnd);
        var len = colorCount;

        //Alpha blending amount
        var alpha = 0.0;

        // Loop len times and return the colors
        var saida = [];
        for (i = 0; i < len; i++) {
            var c = [];
            alpha += (1.0/len);

            c[0] = start[0] * alpha + (1 - alpha) * end[0];
            c[1] = start[1] * alpha + (1 - alpha) * end[1];
            c[2] = start[2] * alpha + (1 - alpha) * end[2];

            saida.push(convertToHex (c));
        }
        return saida;
    }

    Colors.getE4D4Gradient = function (e4Positions, d4Positions) {
        // Create one joint gradient from blue to white to orange; Add grey (878586) in front of array for unknown values
        var d4Gradient = Colors.generateColor('#ffffff', '#00aedb', d4Positions);
        var e4Gradient = Colors.generateColor('#f37735', '#ffffff', e4Positions);
        var e4d4Gradient = d4Gradient.concat(e4Gradient);
        e4d4Gradient.unshift('878586');

        return e4d4Gradient;
    };

    Colors.getOpeningGradient = function (negPositions, posPositions) {
        // Create one joint gradient from blue to white to red; Add grey (878586) in front of array for unknown values
        var negGradient = Colors.generateColor('#ffffff', '#0000ff', negPositions);
        var posGradient = Colors.generateColor('#ff0000', '#ffffff', posPositions);
        var openingGradient = negGradient.concat(posGradient);
        openingGradient.unshift('878586');

        return openingGradient;
    };

    // Optional TODO: Change color to suffix and color based on "W" / "B" or "WON_W" / "WON_B"
    Colors.countryStyle = function (feature, gradient, currentOpening, color){
        return {
            fillColor: '#' + gradient[feature.properties[currentOpening['id'] + '_POS_' + color]],
            weight: 2,
            opacity: 1,
            color: 'grey',
            fillOpacity: 0.9
        };
    }


    global.Colors = Colors;
    })
(window);