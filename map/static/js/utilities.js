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

    function formatCustom(state) {
        return $(
            '<div><div>' + state.text + '</div><div class="foo">'
            + state.pgn
            + '</div></div>'
        );
    }

    Utils.fillDropdown = function (openings) {

        formattedData = []
        for (openingId in openings) {
            formattedData.push({
                'id': openingId,
                'text': openings[openingId]['name'],
                'pgn': openings[openingId]['pgn']
            })
        }

        $(document).ready(function () {
            $('#openingInput').select2({
                placeholder: 'Select an opening',
                data: formattedData,
                initSelection: function (element, callback) {
                },
                templateResult: formatCustom
            });
        });


        // for (openingId in openings) {
        //     var newOption = new Option(openings[openingId]['name'], openingId, false, false);
        //     $('#openingInput').append(newOption);
        // }

    }

    global.Utils = Utils;
})(window);