# Wine and Song Festival ETL

This repository is based on the example from class, in addition to [this
article](https://tegardp.medium.com/the-6-step-etl-process-using-airflow-with-example-and-exercise-db46715a61f0).
To complete the exercise, fill in the missing functions in the
`wine_and_song_festival_etl.py`.

## Running instructions

1. Setup virtualenv for python `python -m venv venv`.
2. Activate virtualenv `source venv/bin/activate`.
3. Install all packages with `pip install -r requirements.txt`.
4. Move all `.py` files to `$AIRFLOW_HOME/dags` (e.g. `/home/<user>/airflow/dags`).
5. Move all data in `raw_data` to `$AIRFLOW_HOME/data/0-raw`.
6. Start the postgres database with `docker-compose up`.
7. Setup an `.env` file with credentials for Azure and Postgres, e.g:

```sh
STORAGE_ACCOUNT_KEY=...
STORAGE_ACCOUNT_NAME=...

DATA_FOLDER=/home/<user>/airflow/data
CONTAINER_NAME=wine-and-song-festival-etl

DATABASE_USER=postgres
DATABASE_PASSWORD=admin
DATABASE_NAME=wine_and_song_festival_etl
```

8. Run the Airflow runner `./runer.sh`.
9. Open the browser to the specified web address and trigger the DAG.
