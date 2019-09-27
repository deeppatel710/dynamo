import flask
from flask import request, jsonify, abort, make_response
import pandas as pd



@app.route('/', methods=['GET'])
def initialize():
	return ("Welcome to the site rehab API")