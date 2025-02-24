
def get_strava_activities(df_cred) :

    # -------------------
    # Var
    # -------------------
    scripts_path = 'scripts/'
    table_name = 'STRAVA_ACTIVITIES_RAW'
    partition_cols = ['athlete.id']


    # -------------------
    # bibli
    # -------------------

    import pandas as pd
    import requests
    import datetime as datetime
    import time

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
    # activities api calls loop
    # -------------------

    three_years_ago = int(time.time()) - (3 * 365 * 24 * 60 * 60)
    activites_url = f"https://www.strava.com/api/v3/athlete/activities?after={three_years_ago}"

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

    page_num = 1
    df_activities_list = []
    len_df_tmp = 1
    while len_df_tmp > 0 :
        print('---------')
        print(f'page num : {page_num}')

        header = {'Authorization': 'Bearer ' + access_token}
        param = {'per_page': 200, 'page': page_num}
        resp_activities = requests.get(activites_url, headers=header, params=param).json()
        df_tmp = pd.json_normalize(resp_activities)
        len_df_tmp = len(df_tmp)
        print('len df : '+str(len_df_tmp))

        if len_df_tmp > 0 :
            df_activities_list.append(df_tmp)

        page_num = page_num + 1

    df_activities = pd.concat(df_activities_list, ignore_index=True)

    # -------------------
    # Uniquement les colonnes que l'on souhaite
    # -------------------
    cols_list = ['resource_state', 'name', 'distance', 'moving_time', 'elapsed_time', 'total_elevation_gain', 'type',
                                'sport_type', 'workout_type', 'id', 'start_date', 'start_date_local', 'timezone', 'utc_offset', 'location_city',
                                'location_state', 'location_country', 'achievement_count', 'kudos_count', 'comment_count', 'athlete_count',
                                'photo_count', 'trainer', 'commute', 'manual', 'private', 'visibility', 'flagged', 'gear_id', 'start_latlng',
                                'end_latlng', 'average_speed', 'max_speed', 'average_cadence', 'average_temp', 'average_watts', 'max_watts',
                                'weighted_average_watts', 'kilojoules', 'device_watts', 'has_heartrate', 'average_heartrate', 'max_heartrate',
                                'heartrate_opt_out', 'display_hide_heartrate_option', 'elev_high', 'elev_low', 'upload_id', 'upload_id_str',
                                'external_id', 'from_accepted_tag', 'pr_count', 'total_photo_count', 'has_kudoed', 'suffer_score', 'athlete.id',
                                'athlete.resource_state', 'map.id', 'map.summary_polyline', 'map.resource_state']
    
    for col in cols_list :
        if col not in df_activities.columns :
            df_activities[col] = None

    df_activities = df_activities[cols_list]

    # -------------------
    # traitement des colonnes de datetime
    # -------------------

    for col in df_activities.columns :
        if 'date' in col :
            df_activities[col] = pd.to_datetime(df_activities[col])


    # ordre alpabetique colonnes
    df_activities = df_activities.reindex(sorted(df_activities.columns), axis=1)



    # -------------------
    # output
    # -------------------
    upload_df_to_cloud_storage(df=df_activities, table_name=table_name, partition_cols=partition_cols)



