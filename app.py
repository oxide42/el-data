from flask import Flask
import core.service

app = Flask(__name__)
core.service.init_app()


@app.route("/api/refresh_data")
def refresh_data():
    core.service.refresh_data()
    return {}


if __name__ == '__main__':
    app.run()
