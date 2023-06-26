var board = null
var game = new Chess()
var $status = $('#status')
var $pgn = $('#pgn')
var currentOpening = null;
var metaData = null
var e4d4Gradient = null
var map = null
var countryLayer = null
var countriesData = null
var openingGradients = null
var openings = null

// code for only allowing legal moves by https://chessboardjs.com/examples#5000
function onDragStart(source, piece, position, orientation) {
    // do not pick up pieces if the game is over
    if (game.game_over()) return false

    // only pick up pieces for the side to move
    if ((game.turn() === 'w' && piece.search(/^b/) !== -1) ||
        (game.turn() === 'b' && piece.search(/^w/) !== -1)) {
        return false
    }
}

function onDrop(source, target) {
    // see if the move is legal
    var move = game.move({
        from: source,
        to: target,
        promotion: 'q' // NOTE: always promote to a queen for example simplicity
    })

    // illegal move
    if (move === null) return 'snapback'

    updateStatus()
    updateOpening()

}

function updateOpeningInDropdown() {

    document.getElementById("openingInput").value = currentOpening[2];

}

function updateOpening() {

    var opening = openings.find(elem => elem[3] == game.pgn());
    console.log(opening)
    if (opening) {
        currentOpening = opening
    } else {
        currentOpening = null
    }
    updateOpeningOnMap()
    updateOpeningInDropdown()

}


// update the board position after the piece snap
// for castling, en passant, pawn promotion
function onSnapEnd() {
    board.position(game.fen())
}

function updateStatus() {
    var status = ''

    var moveColor = 'White'
    if (game.turn() === 'b') {
        moveColor = 'Black'
    }

    // checkmate?
    if (game.in_checkmate()) {
        status = 'Game over, ' + moveColor + ' is in checkmate.'
    }

    // draw?
    else if (game.in_draw()) {
        status = 'Game over, drawn position'
    }

    // game still on
    else {
        status = moveColor + ' to move'

        // check?
        if (game.in_check()) {
            status += ', ' + moveColor + ' is in check'
        }
    }

    $status.html(status)
    $pgn.html(game.pgn())
}

document.addEventListener("DOMContentLoaded", function () {
    // ----------------------------------------------------------------------------
    // Map preparation
    // ----------------------------------------------------------------------------
    // Load necessary data
    Utils.prepareMap().then(arr => {
        metaData = arr[0];

        // Get the names of all recorded openings as well as the gradients
        // (Based on the positions with below and above average probability for each opening)
        // const openings = metaData['openings'];
        Openings.fetchOpenings().then(res => {
            openings = res
            console.log(openings)

            openingGradients = {};
            const negPositions = metaData['negative_positions']
            const posPositions = metaData['positive_positions']

            for (let i = 0; i < openings.length; i++) {
                var name = openings[i][2];
                openingGradients[name] = Colors.getOpeningGradient(negPositions, posPositions)
            }

            Utils.fillDropdown(openings);

            // e4/d4 information
            const e4Positions = metaData['e4_positions'];
            const d4Positions = metaData['d4_positions'];
            countriesData = arr[1];
            e4d4Gradient = Colors.getE4D4Gradient(e4Positions, d4Positions);

            // Add a tile layer (the map background)
            map = L.map('map', {
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

        })

    });

    // chess board

    var config = {
        draggable: true,
        position: 'start',
        onDragStart: onDragStart,
        onDrop: onDrop,
        onSnapEnd: onSnapEnd
    }

    board = Chessboard('board', config)

    updateStatus()

});

function switchToBase() {
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
    if (countryLayer) {
        countryLayer.remove();
    }
    countryLayer = L.geoJSON(countriesData, {
        onEachFeature: Countries.onEachCountryE4D4,
        style: (feature) => Colors.countryStyle(feature, e4d4Gradient, 'E4_D4')
    }).addTo(map);
}

function updateOpeningOnMap() {

    if (!currentOpening) {
        switchToBase()
        return
    }
    if (countryLayer) {
        countryLayer.remove();
    }
    countryLayer = L.geoJSON(countriesData, {
        onEachFeature: (feature, layer) => Countries.onEachCountryOpening(
            feature, layer, currentOpening[2]
        ),
        style: (feature) => Colors.countryStyle(feature, openingGradients[currentOpening[2]], currentOpening[2])
    }).addTo(map);

}

function switchToOpening() {
    if (countryLayer) {
        countryLayer.remove();
    }
    updateOpeningOnMap()
}

function updateOpeningOnBoard() {

    return

}

function openingInputChangedHandler() {

    openingName = document.getElementById('openingInput').value
    currentOpening = openings.find(elem => elem[2] == openingName);
    updateOpeningOnMap()
    updateOpeningOnBoard()

}


