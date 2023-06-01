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

    Utils.prepareMap = async function(){
        const countriesData = await Countries.fetchCountries();
        const metaData = await fetchMetadata();

        return [metaData, countriesData]
    }
    global.Utils = Utils;
})(window);