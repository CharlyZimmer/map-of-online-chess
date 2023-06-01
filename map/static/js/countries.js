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
        } catch (error){
            console.error('Error:', error);
        }
    };

    // --------------------------------------------------------------------------------------------------------------
    // Base function: Show all countries, highlight them with hovering and show name and player numbers on click
    // --------------------------------------------------------------------------------------------------------------
    // Function to interact with countries via the cursor
    Countries.onEachCountryBase = function (feature, layer) {
        layer.on({
            click: countryClickBase,
            mouseover: highlightFeature,
            mouseout: resetHighlight
        });
    }
    // Show country name and player count when clicking
    function countryClickBase(e) {
        alert(e.target.feature.properties.ADMIN + ": " + e.target.feature.properties.PLAYER_COUNT + " players");
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
    global.Countries = Countries;
})(window);