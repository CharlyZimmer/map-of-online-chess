from flask import Flask, render_template

app = Flask(__name__, template_folder='./templates')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/countries')
def leipzig_districts():
    with open('./static/json/countries.geojson', 'r') as file:
        return file.read()


if __name__ == '__main__':
    app.run(debug=True)
