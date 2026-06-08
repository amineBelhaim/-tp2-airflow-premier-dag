# TP 2 — Créer un premier DAG Airflow

Premier DAG Apache Airflow : traduire proprement un workflow simple **Extraction → Transformation → Chargement** (ETL) en un DAG lisible, avec des noms explicites et une tâche par responsabilité.

## Stack

- Apache Airflow 3.2.2
- Python 3.14
- Exécution en local via `airflow standalone` (WSL / Ubuntu)

## Le DAG

Le fichier [`dags/mon_premier_dag_etl.py`](dags/mon_premier_dag_etl.py) définit un DAG `mon_premier_dag_etl` composé de **3 tâches** enchaînées :

```
extraire_donnees  -->  transformer_donnees  -->  charger_donnees
```

Le DAG est déclenché **manuellement** (`schedule=None`).

## Rôle de chaque tâche

| Tâche | Rôle |
|-------|------|
| `extraire_donnees` | Point d'entrée du pipeline. Simule la récupération de données brutes (`[1, 2, 3, 4, 5]`) et les transmet à la tâche suivante via XCom. |
| `transformer_donnees` | Récupère le résultat de l'extraction, applique une transformation (multiplication par 10) et renvoie les données transformées. |
| `charger_donnees` | Étape finale. Récupère les données transformées et simule leur enregistrement (affiche le total et la liste). |

## Dépendances

Les dépendances sont définies **explicitement** par l'opérateur `>>` :

```python
tache_extraction >> tache_transformation >> tache_chargement
```

`>>` signifie « doit s'exécuter avant ». Airflow garantit donc que chaque tâche ne démarre que lorsque la précédente a réussi : on ne transforme qu'après avoir extrait, et on ne charge qu'après avoir transformé.

## Comment l'exécuter

```bash
# 1. Activer l'environnement et lancer Airflow
export AIRFLOW_HOME=~/airflow-tp
source venv/bin/activate
airflow standalone

# 2. Ouvrir l'interface web
#    http://localhost:8080  (login : admin)

# 3. Activer le DAG (toggle) puis cliquer sur "Déclencher"
```

## Comment ça fonctionne (résumé)

Airflow lit le fichier Python du dossier `dags/` et construit un graphe orienté (le **DAG**). Chaque `PythonOperator` exécute une fonction Python. Les données passent d'une tâche à l'autre via le mécanisme **XCom** (`xcom_pull` / valeur de retour). Le **scheduler** orchestre l'ensemble en respectant l'ordre imposé par les dépendances `>>`, et chaque exécution (run) est tracée dans l'interface web avec ses logs.

## Preuve d'exécution

Run manuel terminé avec les 3 tâches en **Succès** :

![Exécution réussie des 3 tâches](screenshots/execution.png)

Logs de la tâche `transformer_donnees` (`Donnees transformees : [10, 20, 30, 40, 50]`) :

![Logs de la tâche](screenshots/logs.png)
