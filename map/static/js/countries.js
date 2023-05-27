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

    // Function to interact with countries via the cursor
    Countries.onEachCountry = function (feature, layer) {
        layer.on({
            click: countryClick,
            mouseover: highlightFeature,
            mouseout: resetHighlight
        });
    }

    // Show country name when clicking
    function countryClick(e) {
        alert("You clicked on " + e.target.feature.properties.ADMIN);
    }

    // Highlight the country when hovered
    function highlightFeature(e) {
        var layer = e.target;
        layer.setStyle({
            weight: 3,
            fillColor: '#666',
            dashArray: '',
            fillOpacity: 0.7
        });

        if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
            layer.bringToFront();
        }
    }

    // Reset the country style when the mouse leaves
    function resetHighlight(e) {
        var layer = e.target;
        layer.setStyle({
            weight: 2,
            fillColor: '#F28F3A',
            fillOpacity: 0.7
        });
    }
    global.Countries = Countries;
})(window);