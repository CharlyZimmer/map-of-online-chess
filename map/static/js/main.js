document.addEventListener("DOMContentLoaded", function () {
    // ----------------------------------------------------------------------------
    // Map preparation
    // ----------------------------------------------------------------------------
    // Load necessary data
    Utils.prepareMap().then(arr => {
        const metaData = arr[0];

        // Get the names of all recorded openings as well as the gradients
        // (Based on the positions with below and above average probability for each opening)
        const openings = metaData['openings'];
        let openingGradients = {};
        for (let i = 0; i < openings.length; i++) {
            var name = openings[i];
            var negPositions = metaData[name + '_neg_positions']
            var posPositions = metaData[name + '_pos_positions']
            openingGradients[name] = Colors.getOpeningGradient(negPositions, posPositions)
        }

        // TODO: Make dropdown invisible and select opening from dropdown
        var openingDropdown = Utils.openingDropdown(openings);
        openingDropdown.style.display = 'block';
        var openingName = 'SICILIAN_DEFENSE';

        // e4/d4 information
        const e4Positions = metaData['e4_positions'];
        const d4Positions = metaData['d4_positions'];
        const countriesData = arr[1];
        var e4d4Gradient = Colors.getE4D4Gradient(e4Positions, d4Positions);

        // Add a tile layer (the map background)
        const map = L.map('map', {
            worldCopyJump: true
        })
            .setView([0, 0], 3);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 19,
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);

        // Add base view to map
        countryLayer = L.geoJSON(countriesData, {
            onEachFeature: Countries.onEachCountryBase,
            style: {
                fillColor: '#F28F3A',
                weight: 2,
                opacity: 1,
                color: 'white',
                fillOpacity: 0.7
            }
        }).addTo(map);

        // ----------------------------------------------------------------------------
        // Buttons to change the map view (base vs. e4d4)
        // ----------------------------------------------------------------------------
        const CustomControl = L.Control.extend({
            onAdd: function () {
                const container = L.DomUtil.create('div', 'custom-control');
                L.DomEvent.disableClickPropagation(container); // Prevent clicks on the control from affecting the map

                const baseButton = L.DomUtil.create('button', '', container);
                baseButton.innerHTML = 'Base';
                baseButton.addEventListener('click', () => switchToBase());

                const e4d4Button = L.DomUtil.create('button', '', container);
                e4d4Button.innerHTML = 'E4/D4 split';
                e4d4Button.addEventListener('click', () => switchToE4D4());

                const openingButton = L.DomUtil.create('button', '', container);
                openingButton.innerHTML = 'Openings';
                openingButton.addEventListener('click', () => switchToOpening());

                return container;
            },
            options: {
                position: 'bottomright'
            }
        });

        map.addControl(new CustomControl());


        function switchToBase() {
            if (document.body.contains(openingDropdown)) {
                document.body.removeChild(openingDropdown);
            }
            if (countryLayer) {
                countryLayer.remove();
            }
            countryLayer = L.geoJSON(countriesData, {
                onEachFeature: Countries.onEachCountryBase,
                style: {
                    fillColor: '#F28F3A',
                    weight: 2,
                    opacity: 1,
                    color: 'white',
                    fillOpacity: 0.7
                }
            }).addTo(map);
        }

        function switchToE4D4() {
            if (document.body.contains(openingDropdown)) {
                document.body.removeChild(openingDropdown);
            }
            if (countryLayer) {
                countryLayer.remove();
            }
            countryLayer = L.geoJSON(countriesData, {
                onEachFeature: Countries.onEachCountryE4D4,
                style: (feature) => Colors.countryStyle(feature, e4d4Gradient, 'E4_D4')
            }).addTo(map);
        }

        function switchToOpening() {
            document.body.appendChild(openingDropdown);
            if (countryLayer) {
                countryLayer.remove();
            }
            countryLayer = L.geoJSON(countriesData, {
                onEachFeature: (feature, layer) => Countries.onEachCountryOpening(
                    feature, layer, openingName
                ),
                style: (feature) => Colors.countryStyle(feature, openingGradients[openingName], openingName)
            }).addTo(map);
        }
    });

    // chess board

    var cfg = {
        position: 'start',
        showNotation: true,
        draggable: true
    };

    // var game = new Chess()

    var board = Chessboard('board', cfg);

});

