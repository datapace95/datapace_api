def get_strava_athlete_zones(df_cred) :

    # -------------------
    # Var
    # -------------------
    scripts_path = 'scripts/'
    table_name = 'STRAVA_ZONES'
    partition_cols = ['athlete_id']

    # -------------------
    # bibli
    # -------------------
    import pandas as pd
    import requests

    import sys
    sys.path.insert(1, scripts_path)
    from upload_to_cloud_storage import upload_df_to_cloud_storage

    # -------------------
    # strava api ids
    # -------------------

    df_cred = df_cred

    client_secret = str(df_cred['client_secret.value'][0])
    client_id = str(df_cred['client_id.value'][0])
    #code = str(df_cred['code.value'][0])
    refresh_token = str(df_cred['refresh_token.value'][0])
    auth_url = str(df_cred['auth_url.value'][0])

    # -------------------
    # api call to get zones
    # -------------------
    athlete_zones_url = "https://www.strava.com/api/v3/athlete/zones"

    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': "refresh_token",
        'f': 'json'
    }

    print("Requesting Token...")
    res = requests.post(auth_url, data=payload, verify=False)
    access_token = res.json()['access_token']
    print('token received')

    header = {'Authorization': 'Bearer ' + access_token}
    resp_athlete_zones = requests.get(athlete_zones_url, headers=header).json()

    # loop to get zone by data type
    rows_for_df = []
    for data_type, details in resp_athlete_zones.items() :
        print('====')
        print(data_type)
        print(details)
        zone_num = 1
        for zone in details['zones'] :
            print('--')
            print('data_type : ' + data_type)
            print('zone_num : ' + str(zone_num))
            print('min : ' + str(zone['min']))
            print('max : ' + str(zone['max']))

            rows_for_df.append({
                'data_type': data_type,
                'zone_num' : str(zone_num),
                'min': str(zone['min']),
                'max': str(zone['max']),
            })

            zone_num = zone_num + 1

    df_zones = pd.DataFrame(rows_for_df)

    athlete_url = "https://www.strava.com/api/v3/athlete"
    df_zones['athlete_id'] = requests.get(athlete_url, headers=header).json()['id']

    df_zones = df_zones[['data_type', 'zone_num', 'min', 'max', 'athlete_id']]

    # -------------------
    # output
    # -------------------
    upload_df_to_cloud_storage(df=df_zones, table_name=table_name, partition_cols=partition_cols)