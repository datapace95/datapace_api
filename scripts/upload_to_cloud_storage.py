from google.oauth2.service_account import Credentials
from google.cloud import storage
import pandas as pd
import sys
import io
from datetime import datetime
from zoneinfo import ZoneInfo

metadata_path = '/metadata/'
gcp_account_file = f'{metadata_path}datapace-190495-7b61bd0a8eb2.json'

def upload_df_to_cloud_storage(df, table_name, partition_cols) :

    print('===============================')
    print('upload_df_to_cloud_storage')
    print('===============================')

    credentials = Credentials.from_service_account_file(gcp_account_file)

    # Initialiser le client Cloud Storage
    client = storage.Client(credentials=credentials)
    bucket_name = 'datapace-bucket'
    bucket = client.bucket(bucket_name)

    # adding update_time
    current_timestamp = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M:%S")
    df['update_time'] = current_timestamp

    #check si cols existent bien
    for p_col in partition_cols :
        if p_col not in df.columns :
            raise ValueError(f"La colonne de partition '{p_col}' n'existe pas dans le DataFrame")
            sys.exit()

    combin_df = df[partition_cols].drop_duplicates().reset_index(drop=True)
    nb_partitions= len(combin_df)

    nb_w = 0
    for _, row in combin_df.iterrows() :

        print('===================')
        d = pd.DataFrame(row).reset_index()

        d.columns = ['col', 'val']


        partition_file_path = '/'
        temp_df = df.copy()
        for _, r in d.iterrows() :
            temp_df = temp_df[temp_df[r['col']] == r['val']]

            partition_file_path = partition_file_path + 'p_' + str(r['col']).replace('.', '_') + '=' + str(r['val']) + '/'


        file_name = table_name + '_' + partition_file_path.replace('/', '_') + '.csv'
        file_name= file_name.replace('_.', '.')

        file_path = 'tables/' + table_name + partition_file_path  + file_name

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, sep=";")
        csv_buffer.seek(0)  # Remettre le pointeur du flux au début

        # Définir le chemin du fichier dans le bucket
        blob = bucket.blob(file_path)

        # Télécharger le contenu du DataFrame dans le bucket
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

        nb_w = nb_w + 1
        print('progress of partitions writing : ' + str(nb_w) + ' / ' + str(nb_partitions), flush=True)