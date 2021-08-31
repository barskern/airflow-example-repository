import os
import re
import logging
import shutil

import pandas as pd
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from azure_api import create_container, create_directory, upload_file_to_directory, download_file_from_directory, list_directory_contents
from database_operations import connect_to_database, create_database, create_table, insert_dataframe


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 1, 1),
    'start_date': days_ago(1),
}

@dag(
    default_args=default_args,
    schedule_interval='@once',
    tags=['wine', 'music', 'song', 'festival'],
)
def wine_and_song_festival_etl():
    """
    A data pipeline for examining the data from the online Wine and Song Festival',
    """
    @task()
    def extract(data_folder, dataset_name, separator=','):
        """
        #### Extract task
        A simple extract task to get data ready for the rest of the data
        pipeline. Can handle csv, json.
        """

        from_path = f"{data_folder}/0-raw/{dataset_name}"
        to_path = f"{data_folder}/1-extract/{dataset_name}.csv"

        # TODO Fill in this function
        pass


    @task(depends_on_past = True)
    def transform_to_songs(data_folder, dataset_name, _):
        """
        #### Transform song_data to songs
        """
        from_path = f"{data_folder}/1-extract/{dataset_name}.csv"
        to_path = f"{data_folder}/2-transform/songs.csv"

        # TODO Fill in this function
        pass


    @task(depends_on_past = True)
    def transform_to_artists(data_folder, dataset_name, _):
        """
        #### Transform song_data to artists
        """
        from_path = f"{data_folder}/1-extract/{dataset_name}.csv"
        to_path = f"{data_folder}/2-transform/artists.csv"

        # TODO Fill in this function
        pass


    @task(depends_on_past = True)
    def transform_to_users(data_folder, dataset_name, x):
        """
        #### Transform user_data to users
        """
        from_path = f"{data_folder}/1-extract/{dataset_name}.csv"
        to_path = f"{data_folder}/2-transform/users.csv"

        # TODO Fill in this function
        pass


    # There is no elegant way to link songplays to songs, due to the lack of a
    # song_id in the user_data, hence we'll simply skip this.
    #
    #@task(depends_on_past = True)
    #def transform_to_songplays(data_folder, dataset_name, x):
    #    """
    #    #### Transform user_data to songplays
    #    """
    #    from_path = f"{data_folder}/1-extract/{dataset_name}.csv"
    #    to_path = f"{data_folder}/2-transform/songplays.csv"
    #    pass


    @task()
    def setup_azure(container_name):
        # TODO Fill in this function
        pass


    @task()
    def setup_sql(database_name):
        # TODO Fill in this function
        pass


    @task(depends_on_past = True)
    def load_to_azure(data_folder, dataset_name, container_name, _):
        """
        #### Load dataset to azure datalake
        """
        from_path = f"{data_folder}/2-transform/{dataset_name}.csv"
        # TODO Fill in this function
        pass


    @task(depends_on_past = True)
    def load_to_sql(data_folder, dataset_name, _):
        """
        #### Load dataset to SQL database
        """
        from_path = f"{data_folder}/2-transform/{dataset_name}.csv"

        # TODO Fill in this function
        pass


    database_name = os.getenv('DATABASE_NAME', 'wine_and_song_festival_etl')
    container_name = os.getenv('CONTAINER_NAME', 'wine-and-song-festival-etl')
    data_folder = os.getenv('DATA_FOLDER', '/home/oruud/airflow/data')

    with TaskGroup('setup') as _:
        w = setup_azure(container_name)
        s = setup_sql(database_name)

    with TaskGroup('extract') as _:
        with TaskGroup('extract_user') as _:
            xu = extract(data_folder, 'user_data')
        with TaskGroup('extract_song') as _:
            xs = extract(data_folder, 'song_data')

    with TaskGroup('transform') as _:
        tu = transform_to_users(data_folder, 'user_data', xu)
        ts = transform_to_songs(data_folder, 'song_data', xs)
        ta = transform_to_artists(data_folder, 'song_data', xs)

        # Unable to use this as we are missing song_id from user data
        #tsp = transform_to_songplays(data_folder, 'user_data', x)

    with TaskGroup('load') as _:
        with TaskGroup('user') as _:
            load_to_azure(data_folder, 'users', container_name, [tu, w])
            su = load_to_sql(data_folder, 'users', [tu, s])

        with TaskGroup('artist') as _:
            load_to_azure(data_folder, 'artists', container_name, [ta, w])
            sa = load_to_sql(data_folder, 'artists', [ta, s])

        with TaskGroup('song') as _:
            load_to_azure(data_folder, 'songs', container_name, [ts, w])
            ss = load_to_sql(data_folder, 'songs', [ts, su, sa])

        #with TaskGroup('songplays') as _:
        #    load_to_azure(data_folder, 'songplays', container_name, [tsp, w])
        #    load_to_sql(data_folder, 'songplays', [tsp, su, sa, ss])

wine_and_song_festival_etl_dag = wine_and_song_festival_etl()
