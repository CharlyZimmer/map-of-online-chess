(async function (global) {
    var Utils = {};

    // Get number of countries with enriched data
    fetchMetadata = async function () {
        try {
            const response = await fetch('./enrichment_metadata');
            if (response.ok) {
                return await response.json();
            } else {
                throw new Error('Error fetching metadata JSON');
            }
        } catch (error) {
            console.error('Error:', error);
        }
    };

    Utils.prepareMap = async function () {
        const countriesData = await Countries.fetchCountries();
        const metaData = await fetchMetadata();

        return [metaData, countriesData]
    }

    Utils.cleanOpeningName = openingName =>
        openingName.replaceAll(/\S*/g, word =>
            `${word.slice(0, 1)}${word.slice(1).toLowerCase()}`
                .replaceAll('_', ' ')
        );

    Utils.fillDropdown = function (openings) {
        var datalist = document.getElementById('openingSuggestions')

        // for (openingId in openings) {
        //     var newOption = document.createElement("option");
        //     newOption.value = openings[openingId]['name']

        //     datalist.appendChild(newOption);
        // };

        options = ''
        for (openingId in openings) {
            options += '<option value="' + openings[openingId]['name'] + '" data-value="' + openingId + '" />';
        }

        datalist.innerHTML = options;

    }

    global.Utils = Utils;
})(window);