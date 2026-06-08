from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extraire_donnees():
    """Etape 1 : simule la recuperation de donnees brutes."""
    donnees = [1, 2, 3, 4, 5]
    print(f"Donnees extraites : {donnees}")
    return donnees


def transformer_donnees(**context):
    """Etape 2 : transforme les donnees extraites."""
    donnees = context["ti"].xcom_pull(task_ids="extraire_donnees")
    resultat = [x * 10 for x in donnees]
    print(f"Donnees transformees : {resultat}")
    return resultat


def charger_donnees(**context):
    """Etape 3 : simule l'enregistrement du resultat final."""
    resultat = context["ti"].xcom_pull(task_ids="transformer_donnees")
    print(f"Donnees chargees (total = {sum(resultat)}) : {resultat}")


with DAG(
    dag_id="mon_premier_dag_etl",
    description="Premier DAG simple : extraction, transformation, chargement",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tp", "demo"],
) as dag:

    tache_extraction = PythonOperator(
        task_id="extraire_donnees",
        python_callable=extraire_donnees,
    )

    tache_transformation = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer_donnees,
    )

    tache_chargement = PythonOperator(
        task_id="charger_donnees",
        python_callable=charger_donnees,
    )

    # Dependances explicites : extraction -> transformation -> chargement
    tache_extraction >> tache_transformation >> tache_chargement
