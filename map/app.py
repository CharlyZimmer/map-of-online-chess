from flask import Flask, render_template

app = Flask(__name__, template_folder="./templates")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/countries")
def countries_geojson():
    with open("./static/json/countries.geojson", "r") as file:
        return file.read()


@app.route("/enrichment_metadata")
def metadata():
    with open("./static/json/enrichment_metadata.json", "r") as file:
        return file.read()


if __name__ == "__main__":
    app.run(debug=True)
