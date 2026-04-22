import requests
import json
from datetime import datetime
from pathlib import Path


URL = "https://opensky-network.org/api/states/all"


def run_bronze_ingest(**context):
    # ÉTAPE 1 : on récupère les données de l'API
    response = requests.get(URL , timeout = 30)
    response.raise_for_status()

    data = response.json()
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


    # ÉTAPE 2 : on prépare le chemin du fichier
    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    # ÉTAPE 3 : on sauvegarde les données dans le fichier
    with open(path, "w") as f:
       json.dump(data, f, indent=4)

    # ÉTAPE 4 : on annonce aux autres tâches où est le fichier
    context['ti'].xcom_push(key="bronze_file", value=str(path))