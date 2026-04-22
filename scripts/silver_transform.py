import json 
import pandas as pd
from pathlib import Path


def run_silver_transform(**context):
    execution_date = context['ds_nodash']
    # ÉTAPE 1 : on récupère le chemin du fichier bronze
    
    silver_path = Path(f"/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)
    bronze_file = context['ti'].xcom_pull(key="bronze_file", task_ids="bronze_ingest")

    if not bronze_file :
        raise ValueError("Bronze file path not found in XCom for date: {}".format(execution_date))

    # ÉTAPE 2 : on lit le fichier et on transforme les données
    with open(bronze_file, "r") as f:
        raw = json.load(f)

    df_raw = pd.DataFrame(raw["states"])
    df_raw.columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude",
        "latitude", "baro_altitude", "on_ground", 
        "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk",
        "spi", "position_source"
    ]
     

     # selecting the columns that we need and we left others
    df = df_raw[
       [
        "icao24",           # identifiant de l'avion
        "origin_country",   # pays d'origine
        "velocity",         # vitesse
        "on_ground"         # est-il au sol ?
       ]
    ]


    # ÉTAPE 3 : on prépare le chemin du fichier silver
    output_file = Path(f"/opt/airflow/data/silver/flights_{execution_date}.csv")

    # ÉTAPE 4 : on sauvegarde les données transformées dans un fichier CSV
    df.to_csv(output_file, index=False)

    # ÉTAPE 5 : on annonce aux autres tâches où est le fichier silver
    context['ti'].xcom_push(key="silver_file", value=str(output_file))
