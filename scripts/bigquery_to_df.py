
def bigquery_to_df(query_string):

    from google.oauth2.service_account import Credentials
    from google.cloud import bigquery

    # Initialiser les identifiants à partir du fichier JSON
    metadata_path = '/metadata/'
    gcp_account_file = f'{metadata_path}datapace-190495-7b61bd0a8eb2.json'
    credentials = Credentials.from_service_account_file(gcp_account_file)

    # Initialiser le client BigQuery
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Exécuter la requête et récupérer les résultats sous forme de DataFrame
    query_job = client.query(query_string)
    result = query_job.result()  # Exécuter la requête et attendre le résultat
    df = result.to_dataframe()  # Convertir le résultat en DataFrame

    return df