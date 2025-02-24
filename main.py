### ==========================================================================================
### IMPORT 1
### ==========================================================================================
import pandas as pd
import json


### ==========================================================================================
### VAR
### ==========================================================================================
metadata_path = '/metadata/'
config_file_path = f'{metadata_path}config.json'

with open(config_file_path, "r") as read_file:
    data_read = json.load(read_file)
df_cred_datapace_api = pd.json_normalize(data_read['datapace_api'])
DATAPACE_API_KEY = str(df_cred_datapace_api['api_key.value'][0])

scripts_path = 'scripts/'

### ==========================================================================================
### FUNCTIONS
### ==========================================================================================
def get_cred_strava_api() :
    with open(config_file_path, "r") as read_file:
        data_read = json.load(read_file)
    df_cred_strava_api = pd.json_normalize(data_read['strava_api'])
    return df_cred_strava_api

### ==========================================================================================
### IMPORT 2
### ==========================================================================================
from flask import Flask, request, jsonify

import sys
sys.path.insert(1, scripts_path)
#from get_strava_api_infos import get_strava_api_infos
from get_strava_activities import get_strava_activities
from get_strava_athlete_zones import get_strava_athlete_zones
from get_strava_activity_streams import get_strava_activity_streams

### ==========================================================================================
### APP
### ==========================================================================================
app = Flask(__name__)

@app.before_request
def check_api_key():
    api_key = request.headers.get("x-api-key")
    if api_key != DATAPACE_API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    
@app.route("/")
def home():
    return "Welcome to Datapace API!"

@app.route("/get_strava_raw_data", methods=["POST"])
def get_strava_raw_data() :

    # obtenir le refresh token de strava
    # print('-------------------------------')
    # print("get strava api refresh_token with df_strava_api_infos function...")
    # df_strava_api_infos = get_strava_api_infos()
    # print("refresh_token received")

    # charger credentials strava api
    df_cred_strava_api = get_cred_strava_api()

    #
    print('-------------------------------')
    print("get_strava_activities function is running...")
    get_strava_activities(df_cred=df_cred_strava_api)
    print("get_strava_activities completed")

    print('-------------------------------')
    print("get_strava_athlete_zones function is running...")
    get_strava_athlete_zones(df_cred=df_cred_strava_api)
    print("get_strava_athlete_zones completed")

    print('-------------------------------')
    print("get_strava_athlete_zones function is running...")
    get_strava_activity_streams(df_cred=df_cred_strava_api, which='all')
    print("get_strava_athlete_zones completed")

    return "get_strava_raw_data finished."

if __name__ == "__main__":
    app.run(host="0.0.0.0" , debug=True, port=8080)