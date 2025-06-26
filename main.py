### ==========================================================================================
### IMPORT 1
### ==========================================================================================
import pandas as pd
import json
import requests


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
    # Ne pas vérifier l'API key pour le webhook Strava
    if request.path == "/strava_webhook":
        return

    api_key = request.headers.get("x-api-key")
    if api_key != DATAPACE_API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    
@app.route("/")
def home():
    return "Welcome to Datapace API!"

@app.route("/get_strava_raw_data", methods=["POST"])
def get_strava_raw_data() :

    body = request.get_json()
    object_id = body.get("object_id") if body else None
    print(f"Received object_id: {object_id}")

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
    print("get_strava_activity_streams function is running...")
    get_strava_activity_streams(df_cred=df_cred_strava_api, which='all', activity_id = object_id)
    print("get_strava_activity_streams completed")

    return "get_strava_raw_data finished."

@app.route("/strava_webhook", methods=["GET", "POST"])
def strava_webhook():
    if request.method == "GET":
        # Validation initiale de l’URL par Strava
        verify_token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        mode = request.args.get("hub.mode")

        if verify_token == "STRAVA" and mode == "subscribe":
            return jsonify({"hub.challenge": challenge}), 200
        else:
            return jsonify({"error": "Verification failed"}), 403

    elif request.method == "POST":
        event = request.json
        print(f"Strava webhook received: {event}")
        if event.get("object_type") == 'activity' :
            object_id = event.get("object_id")
            print(f"Object ID = {object_id}")
            # call api sur get_strava_raw_data
            resp = requests.post(
                        "https://datapace-api-424544346751.us-central1.run.app/get_strava_raw_data",
                        headers={
                            "User-Agent": "App-Strava-Webhook-Endpoint",
                            "x-api-key": DATAPACE_API_KEY
                        },
                        json={"object_id": object_id}
                    )
            print("reponse call api interne :", resp.status_code)
        else :
            print("Webhook ignored (not an activity event)")
        return "", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0" , debug=True, port=8080)