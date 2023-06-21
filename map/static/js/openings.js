(async function (global) {
    var Openings = {};

    // fetch Opening pgn data
    Openings.fetchOpenings = async function () {
        try {
            const response = await fetch('./openings');
            if (response.ok) {
                return await response.text();
            } else {
                throw new Error('Error fetching openings CSV');
            }
        } catch (error) {
            console.error('Error:', error);
        }
    };

    global.Openings = Openings;
})(window);