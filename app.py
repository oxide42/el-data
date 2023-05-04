from flask import Flask, render_template, Blueprint, request
import core.service

app = Flask(__name__)

core.service.init_app()

if __name__ == '__main__':
    app.run()
