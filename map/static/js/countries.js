(async function (global) {
    var Countries = {};

    // Fetch country JSON data
    Countries.fetchCountries = async function () {
        try {
            const response = await fetch('./countries');
            if (response.ok) {
                return await response.json();
            } else {
                throw new Error('Error fetching country JSON');
            }
        } catch (error) {
            console.error('Error:', error);
        }
    };

    // --------------------------------------------------------------------------------------------------------------
    // Base function: Highlight countries with hovering and show name and player numbers on click
    // --------------------------------------------------------------------------------------------------------------
    // Function to interact with countries via the cursor
    Countries.onEachCountryBase = function (feature, layer, map) {
        layer.on({
            click: (e) => countryClickBase(e, map),
            mouseover: highlightFeature,
            mouseout: resetHighlight
        });
    }
    // Show country name and player count when clicking
    function countryClickBase(e, map) {
        var popup = L.popup()
            .setLatLng(e.latlng)
            .setContent('<div style="width: 200px;">'
                + "<div style='font-weight: bold'>" + e.target.feature.properties.ADMIN + "</div><br>"
                + "<div style='float:left'>Players:</div>"
                + "<div style='float:right; text-align: right'>" + e.target.feature.properties.PLAYER_COUNT.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + "</div><br>"
                + '</div>')
            .openOn(map);
    }

    // --------------------------------------------------------------------------------------------------------------
    // E4D4 function: In addition to base functions, also show E4 and D4 values on click
    // --------------------------------------------------------------------------------------------------------------
    Countries.onEachCountryE4D4 = function (feature, layer) {
        layer.on({
            click: countryClickE4D4,
            mouseover: highlightFeatureOpening,
            mouseout: resetHighlightOpening
        });
    }
    function countryClickE4D4(e) {
        alert(e.target.feature.properties.ADMIN + ": " + e.target.feature.properties.PLAYER_COUNT + " players\n" +
            "- E4 share: " + Math.round(e.target.feature.properties.E4 * 1000) / 10 + "%\n" +
            "- D4 share: " + Math.round(e.target.feature.properties.D4 * 1000) / 10 + "%");
    }

    // --------------------------------------------------------------------------------------------------------------
    // Opening function: In addition to base functions, also show the probability for an opening on click
    // --------------------------------------------------------------------------------------------------------------
    Countries.onEachCountryOpening = function (feature, layer, currentOpening, color, map) {
        layer.on({
            click: (e) => countryClickOpening(e, currentOpening, color, map),
            mouseover: highlightFeatureOpening,
            mouseout: resetHighlightOpening
        });
    }

    function countryClickOpening(e, currentOpening, color, map) {

        var popup = L.popup()
            .setLatLng(e.latlng)
            .setContent('<div style="width: 200px;">'
                + "<div style='font-weight: bold'>" + e.target.feature.properties.ADMIN + "</div><br>"
                + "<div style='float:left'>Players:</div>"
                + "<div style='float:right; text-align: right'>" + e.target.feature.properties.PLAYER_COUNT.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + "</div><br>"
                + "<div style='float:left'>Probability:</div>"
                + "<div style='float:right; text-align: right'>" + Math.round(e.target.feature.properties[currentOpening['id'] + "_" + color] * 1000) / 10 + "%</div><br>"
                + "<div style='float:left'>Win:</div>"
                + "<div style='float:right; text-align: right'>" + Math.round(e.target.feature.properties[currentOpening['id'] + "_WON_" + color] * 1000) / 10 + "%</div><br>"
                + '</div>')
            .openOn(map);

    }


    // --------------------------------------------------------------------------------------------------------------
    // Helper functions
    // --------------------------------------------------------------------------------------------------------------
    // Highlight the country when hovered
    function highlightFeature(e) {
        var layer = e.target;
        layer.setStyle({
            fillOpacity: 1
        });

        if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
            layer.bringToFront();
        }
    }
    // Reset the country style when the mouse leaves
    function resetHighlight(e) {
        var layer = e.target;
        layer.setStyle({
            fillOpacity: 0.7
        });
    }

    function highlightFeatureOpening(e) {
        var layer = e.target;
        layer.setStyle({
            fillOpacity: 1,
            weight: 3,
            color: 'white',
        });
        if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
            layer.bringToFront();
        }
    }

    function resetHighlightOpening(e) {
        var layer = e.target;
        layer.setStyle({
            fillOpacity: 0.9,
            weight: 2,
            color: 'grey',
        });
    }

    global.Countries = Countries;
})(window);