from flask import Flask, render_template, send_file

app = Flask(__name__, template_folder="./templates")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/countries")
def countries_geojson():
    with open("static/json/countries.geojson", "r") as file:
        return file.read()


@app.route("/enrichment_metadata")
def metadata():
    with open("static/json/enrichment_metadata.json", "r") as file:
        return file.read()


@app.route("/openings")
def openings():
    with open("../data/openings/openings.csv", "r") as file:
        return file.read()


@app.route("/img/chesspieces/wikipedia/<string:piece>")
def img(piece):
    return send_file(
        "./static/img/chesspieces/wikipedia/" + piece, mimetype="image/png"
    )


if __name__ == "__main__":
    app.run(debug=True)
