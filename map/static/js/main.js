var board = null
var $board = $('#board')
var squareClass = 'square-55d63'
var game = new Chess()
var $pgn = $('#pgnContent')
var currentOpening = null;
var metaData = null
var e4d4Gradient = null
var map = null
var countryLayer = null
var countriesData = null
var openingGradients = null
//TODO: Change color dynamically
var color = "W"

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
        promotion: 'q'
    })

    // illegal move
    if (move === null) return 'snapback'

    $board.find('.' + squareClass).removeClass('highlight')
    $board.find('.square-' + move.from).addClass('highlight')
    $board.find('.square-' + move.to).addClass('highlight')

    updateStatus()
    updateOpeningAfterMove()

}

function updateOpeningInDropdown() {

    if (!currentOpening) {
        $('#openingInput').val("")
        $('#openingInput').trigger('change.select2');
        return
    }

    $('#openingInput').val(currentOpening['id'])
    $('#openingInput').trigger('change.select2');

}

function updateOpeningAfterMove() {

    currentOpening = null

    for (openingId in metaData['openings']) {
        elem = metaData['openings'][openingId]
        if (elem['pgn'] == game.pgn()) {
            currentOpening = { 'id': openingId, 'data': elem }
            break
        }
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
        const negPositions = metaData['negative_positions']
        const posPositions = metaData['positive_positions']

        openingGradients = Colors.getOpeningGradient(negPositions, posPositions)

        Utils.fillDropdown(metaData['openings']);

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

        var legend = L.control({ position: 'bottomleft' });

        legend.onAdd = function (map) {
            var div = L.DomUtil.create("div", "legend");
            div.innerHTML += `
            <div style="background-color: white; opacity: 95%; height: 200px; width: 80px; padding-top: 20px;">
                <div style="display: inline-block; height: 160px; width: 100%">
                    <div style="float: left; display: inline-block; height: 100%; width: 20px; margin-left: 15px; background: linear-gradient(to bottom, #ff0000 0%, #ffffff 50%, #0000ff 100%);"></div>
                    <div style="float: right; display: grid; height: 100%; width: 30px; margin-right: 7px;">
                        <div style="margin-top: -6px">50%</div>
                        <div style="line-height: 108px; margin-bottom: -13px">34%</div>
                        <div style="display: flex; height: 100%; ">
                            <div style="display: inline-block; align-self: flex-end; margin-bottom: -6px">12%</div>
                        </div>
                    </div>
                </div>
            </div>`;
            return div;
        };

        legend.addTo(map);

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
    reset()
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
            feature, layer, currentOpening, color
        ),
        style: (feature) => Colors.countryStyle(feature, openingGradients, currentOpening, color)
    }).addTo(map);

}

function updateOpeningOnBoard() {

    game.reset()
    game.load_pgn(currentOpening['data']['pgn'])

    board.start()

    lastMove = null
    moveHistory = game.history({ verbose: true });
    while (moveHistory.length > 0) {
        var p1Move = moveHistory.shift(),
            p2Move = moveHistory.shift(),
            p1c = p1Move.from + '-' + p1Move.to,
            p2c = (p2Move == undefined) ? '' : p2Move.from + '-' + p2Move.to;
        lastMove = (p2Move == undefined) ? p1Move : p2Move
        board.move(p1c)
        board.move(p2c)
        i++;
    }

    $board.find('.' + squareClass).removeClass('highlight')
    $board.find('.square-' + lastMove.from).addClass('highlight')
    $board.find('.square-' + lastMove.to).addClass('highlight')

    updateStatus()

}

function reset() {

    currentOpening = null
    board.start()
    game.reset()
    $('#openingInput').val("")
    $('#openingInput').trigger('change.select2');
    $board.find('.' + squareClass).removeClass('highlight')
    switchToBase()
    updateStatus()

}

$('#openingInput').on('select2:select', function (e) {

    selectedOption = $('#openingInput').find(':selected');

    if (!selectedOption) {
        currentOpening = null
        switchToBase()
        board.start()
        game.reset()
        updateStatus()
        return
    }
    newId = selectedOption.val()
    currentOpening = { 'id': newId, 'data': metaData['openings'][newId] };
    changeColorAfterDropdownChange()
    updateOpeningOnMap()
    updateOpeningOnBoard()

})

function changeColor(checkbox) {

    if (checkbox.checked) {
        color = "B"
    } else {
        color = "W"
    }

    updateOpeningOnMap()

}

function changeColorAfterDropdownChange() {

    // preselect color based on the last move that was played
    // count number of spaces in last turn
    // 1 space = last move was white
    // 2 spaces = last move was black
    turns = currentOpening.data.pgn.split(".")
    lastTurn = turns[turns.length - 1]
    numberOfSpaces = (lastTurn.match(/ /g) || []).length
    if (numberOfSpaces == 1) {
        color = "W"
    } else {
        color = "B"
    }

    updateColorToggle()

}

function updateColorToggle() {

    if (color == "W") {
        $('#colorCheckbox').prop("checked", false)
        return
    }
    $('#colorCheckbox').prop("checked", true)

}