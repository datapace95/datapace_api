
def get_strava_activity_streams(which, df_cred) :

    # -------------------
    # Var
    # -------------------
    scripts_path = 'scripts/'
    table_name = 'STRAVA_ACTIVITY_STREAMS_RAW'
    partition_cols = ['athlete_id', 'activity_id']
    nb_max_streams_to_get = 50 # utiliser pour eviter d'ateindre limit api

    # -------------------
    # bibli
    # -------------------
    import pandas as pd
    import requests

    import sys
    sys.path.insert(1, scripts_path)
    from upload_to_cloud_storage import upload_df_to_cloud_storage
    from bigquery_to_df import bigquery_to_df

    df_cred = df_cred

    client_secret = str(df_cred['client_secret.value'][0])
    client_id = str(df_cred['client_id.value'][0])
    #code = str(df_cred['code.value'][0])
    refresh_token = str(df_cred['refresh_token.value'][0])
    auth_url = str(df_cred['auth_url.value'][0])

    # -------------------
    # get atlete_id
    # -------------------

    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': "refresh_token",
        'f': 'json'}

    print("Requesting Token...")
    res = requests.post(auth_url, data=payload, verify=False)
    access_token = res.json()['access_token']
    print("Access Token = {}".format(access_token))

    header = {'Authorization': 'Bearer ' + access_token}

    athlete_url = "https://www.strava.com/api/v3/athlete"
    athlete_id = requests.get(athlete_url, headers=header).json()['id']


    # -------------------
    # uniques activity_id from ACTIVITIES
    # -------------------
    if which == 'all' :
        sql_query = f"""SELECT id FROM `tables.STRAVA_ACTIVITIES_RAW` WHERE p_athlete_id = {athlete_id}"""
    elif which == 'only_after_last' :
        sql_query = f"""
            WITH _t AS (
            SELECT
            id, start_date_local, name, sas.p_activity_id
            FROM `tables.STRAVA_ACTIVITIES_RAW` sa
            LEFT JOIN (
            SELECT DISTINCT p_activity_id, p_athlete_id FROM `tables.STRAVA_ACTIVITY_STREAMS_RAW` WHERE p_athlete_id = {athlete_id}
            ) sas ON sa.p_athlete_id = sas.p_athlete_id AND sa.id = sas.p_activity_id
            WHERE sa.p_athlete_id = {athlete_id}
            )
            ,

            _last_get AS (
            SELECT
            start_date_local
            FROM _t
            WHERE p_activity_id IS NOT NULL
            ORDER BY start_date_local DESC
            LIMIT 1
            )

            SELECT 
            id
            FROM _t
            CROSS JOIN _last_get
            WHERE _t.start_date_local > _last_get.start_date_local
            """
    df_activities = bigquery_to_df(query_string=sql_query)
    df_activities = df_activities.sort_values(by='id', ascending=False)
    activity_id_list_from_activities = df_activities['id'].unique().tolist()

    # -------------------
    # uniques activity_id from ACTIVITY_STREAMS_RAW, already get
    # -------------------
    try :
        df_streams = bigquery_to_df(query_string=f"SELECT DISTINCT p_activity_id FROM `tables.STRAVA_ACTIVITY_STREAMS_RAW` WHERE p_athlete_id = {athlete_id}")
        activity_id_list_from_streams = df_streams['p_activity_id'].unique().tolist()
    except :
        df_streams = pd.DataFrame()
        activity_id_list_from_streams = []

    # -------------------
    # activity ids to get
    # -------------------
    activity_ids_to_get_list = list([])

    for e in activity_id_list_from_activities :
        if e not in activity_id_list_from_streams :
            activity_ids_to_get_list.append(e)

    # -------------------
    # main loop to get streams by api calls
    # -------------------    
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': "refresh_token",
        'f': 'json'}

    print("Requesting Token...")
    res = requests.post(auth_url, data=payload, verify=False)
    access_token = res.json()['access_token']
    print("Access Token = {}".format(access_token))

    #df_activity_stream_norm_list = []

    nb_activities = len(activity_ids_to_get_list)
    activity_num = 1

    print(activity_ids_to_get_list)
    if len(activity_ids_to_get_list) > 0 :
        for activity_id in activity_ids_to_get_list :

            if activity_num < nb_max_streams_to_get :

                print('=============')
                print("activity_id "+str(activity_id)+" "+str(activity_num)+"/"+str(min(nb_activities, nb_max_streams_to_get)))

                stream_url = "https://www.strava.com/api/v3/activities/"+str(activity_id)+"/streams?keys=time,distance,altitude,velocity_smooth,heartrate,cadence,watts,temp,moving,grade_smooth&series_type=time"

                header = {'Authorization': 'Bearer ' + access_token}
                param = {'per_page': 200, 'page': 1}
                resp_activity_stream = requests.get(stream_url, headers=header, params=param).json()
                df_tmp = pd.json_normalize(resp_activity_stream)
                #display(df_tmp)
                len_df_tmp = len(df_tmp)
                print('len df : '+str(len_df_tmp))
                

                try :
                    moving_list = df_tmp[df_tmp['type'] == 'moving'].reset_index()['data'][0]
                except KeyError :
                    moving_list = None
                    
                try :
                    latlng_list = df_tmp[df_tmp['type'] == 'latlng'].reset_index()['data'][0]
                except KeyError :
                    latlng_list = None
                
                try :
                    velocity_smooth_list = df_tmp[df_tmp['type'] == 'velocity_smooth'].reset_index()['data'][0]
                except KeyError :
                    velocity_smooth_list = None
                
                try :
                    grade_smooth_list = df_tmp[df_tmp['type'] == 'grade_smooth'].reset_index()['data'][0]
                except :
                    grade_smooth_list = None
                
                try :
                    distance_list = df_tmp[df_tmp['type'] == 'distance'].reset_index()['data'][0]
                except KeyError :
                    distance_list = None
                
                try :
                    altitude_list = df_tmp[df_tmp['type'] == 'altitude'].reset_index()['data'][0]
                except KeyError :
                    altitude_list = None
                
                try :
                    heartrate_list = df_tmp[df_tmp['type'] == 'heartrate'].reset_index()['data'][0]
                except KeyError :
                    heartrate_list = None
                
                try :
                    time_list = df_tmp[df_tmp['type'] == 'time'].reset_index()['data'][0]
                except KeyError :
                    time_list = None
                    
                try :
                    watts_list = df_tmp[df_tmp['type'] == 'watts'].reset_index()['data'][0]
                except KeyError :
                    watts_list = None
                    
                df_activity_stream_norm_tmp = pd.DataFrame()
            
                df_activity_stream_norm_tmp['moving'] = moving_list
                df_activity_stream_norm_tmp['latlng'] = latlng_list
                df_activity_stream_norm_tmp['velocity_smooth'] = velocity_smooth_list
                df_activity_stream_norm_tmp['grade_smooth'] = grade_smooth_list
                df_activity_stream_norm_tmp['distance'] = distance_list
                df_activity_stream_norm_tmp['altitude'] = altitude_list
                df_activity_stream_norm_tmp['heartrate'] = heartrate_list
                df_activity_stream_norm_tmp['time'] = time_list
                df_activity_stream_norm_tmp['watts'] = watts_list

                print("series tyyype")
                print(df_tmp['series_type'][0])
            
                df_activity_stream_norm_tmp['series_type'] = df_tmp['series_type'][0]
                df_activity_stream_norm_tmp['original_size'] = df_tmp['original_size'][0]
                df_activity_stream_norm_tmp['resolution'] = df_tmp['resolution'][0]
                df_activity_stream_norm_tmp['activity_id'] = activity_id
                df_activity_stream_norm_tmp['athlete_id'] = athlete_id

                # -------------------
                # create unique ids in df_activity_stream_norm and check if uniques
                # -------------------
                df_activity_stream_norm_tmp['id'] = df_activity_stream_norm_tmp['activity_id'].astype(str).str.cat(df_activity_stream_norm_tmp['time'].astype(str), sep='_')

                # -------------------
                # Uniquement les colonnes que l'on souhaite
                # -------------------
                cols_list = ['moving', 'latlng', 'velocity_smooth', 'grade_smooth', 'distance', 'altitude', 'heartrate', 'time', 'watts', 'series_type',
                            'original_size', 'resolution', 'activity_id', 'athlete_id', 'id']
                
                for col in cols_list :
                    if col not in df_activity_stream_norm_tmp.columns :
                        df_activity_stream_norm_tmp[col] = None

                df_activity_stream_norm_tmp = df_activity_stream_norm_tmp[cols_list]

                # -------------------
                # output
                # -------------------
                #write_data(df=df_activity_stream_norm, table_name=table_name, partition_cols=['athlete_id', 'activity_id'])
                upload_df_to_cloud_storage(df=df_activity_stream_norm_tmp, table_name=table_name, partition_cols=partition_cols)
                activity_num = activity_num + 1
    else :
        print("Aucune activité à récupérer depuis la dernière déjà récupérée.")








                    
    #         #display(df_activity_stream_norm_tmp)
                    
    #         df_activity_stream_norm_list.append(df_activity_stream_norm_tmp)
            
    #         activity_num = activity_num + 1

    # df_activity_stream_norm = pd.concat(df_activity_stream_norm_list, ignore_index=True)  

    # # -------------------
    # # create unique ids in df_activity_stream_norm and check if uniques
    # # -------------------
    # df_activity_stream_norm['id'] = df_activity_stream_norm['activity_id'].astype(str).str.cat(df_activity_stream_norm['time'].astype(str), sep='_')
    # #df_activity_stream_norm = df_activity_stream_norm.drop_duplicates(subset=['id'])
    # duplicates = df_activity_stream_norm['id'].duplicated()
    # if duplicates.any():
    #     print('There are duplicate values in "id" column')
    #     check_unique_in_new_streams = 'NOK'
    #     sys.exit()
    # else:
    #     check_unique_in_new_streams = 'ok'

    # # -------------------
    # # force format
    # # -------------------
    # #df_activity_stream_norm['distance'] = df_activity_stream_norm['distance'].astype('float64')
    # #df_activity_stream_norm['heartrate'] = df_activity_stream_norm['heartrate'].astype('float64')
    # #df_activity_stream_norm['watts'] = df_activity_stream_norm['watts'].astype('float64')
    # #df_activity_stream_norm['time'] = df_activity_stream_norm['time'].astype('float64')


    # # # -------------------
    # # # append with data already called
    # # # -------------------
    # # try :
    # #     # check if unique ids
    # #     if len(df_streams) > 0 :
    # #         df_streams['id'] = df_streams['activity_id'].astype(str).str.cat(df_streams['time'].astype(str)
    # #                                                                         , sep='_')
    # #         duplicates = df_streams['id'].duplicated()
    # #         if duplicates.any():
    # #             print('There are duplicate values in "id" column')
    # #             check_unique_in_streams = 'NOK'
    # #             sys.exit()
    # #         else:
    # #             check_unique_in_streams = 'ok'
    # #     else :
    # #         check_unique_in_streams = 'ok'
        
    # #     # APPEND
    # #     if check_unique_in_new_streams == 'ok' and check_unique_in_streams == 'ok' :
    # #         df_streams_new = pd.concat([df_streams, df_activity_stream_norm])
            
    # # except :
    # #     print('append between old and new failed')
    # #     insert_error_job_state(root_jobname_var, current_script_var, 'append between old and new failed')

    # # # -------------------
    # # # check if no duplicates
    # # # -------------------
    # # try :
    # #     duplicates = df_streams_new['id'].duplicated()
    # #     if duplicates.any():
    # #         print('There are duplicate values in "id" column')
    # #         check_unique_in_new_streams = 'NOK'
    # #         sys.exit()
    # #     else:
    # #         check_unique_in_new_streams = 'ok'
    # # except :
    # #     print('duplicates after append')
    # #     insert_error_job_state(root_jobname_var, current_script_var, 'duplicates after append')

    # # -------------------
    # # Uniquement les colonnes que l'on souhaite
    # # -------------------
    # cols_list = ['moving', 'latlng', 'velocity_smooth', 'grade_smooth', 'distance', 'altitude', 'heartrate', 'time', 'watts', 'series_type',
    #              'original_size', 'resolution', 'activity_id', 'athlete_id', 'id']
    
    # for col in cols_list :
    #     if col not in df_activity_stream_norm.columns :
    #         df_activity_stream_norm[col] = None

    # df_activity_stream_norm = df_activity_stream_norm[cols_list]

    # # -------------------
    # # output
    # # -------------------

    # #write_data(df=df_activity_stream_norm, table_name=table_name, partition_cols=['athlete_id', 'activity_id'])
    # upload_df_to_cloud_storage(df=df_activity_stream_norm, table_name=table_name, partition_cols=partition_cols)

if __name__ == '__main__' :
    get_strava_activity_streams(refresh_token='890073e6a0daf97cf6b520ddd5346e58a7d87e29', athlete_id='31696348', which='only_after_last')