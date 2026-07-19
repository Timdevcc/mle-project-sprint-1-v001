from airflow.decorators import dag, task
from notifications.message import send_telegram_failure_message, send_telegram_success_message
import pendulum
import pandas as pd

def remove_duplicates(data):
    feature_cols = data.columns.drop('id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep='first')
    data = data[~is_duplicated_features].reset_index(drop=True)
    return data 

def remove_missing_values(data):
    return data.dropna().reset_index(drop=True)

def remove_outliers(data):
    potential_outliers = pd.DataFrame()
    num_cols = data.select_dtypes(['float']).columns
    threshold = 3
    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = IQR * threshold
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)
    outliers = potential_outliers.any(axis=1)
    data = data[~outliers]
    return data

@dag(
    schedule="@once",
    dag_id = 'clean_flats_price_dataset',
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    catchup=False,
    tags=['ETL'],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def clean_flats_price_dataset():
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        from sqlalchemy import (
            MetaData, Table, Column, String, Float,
            Integer, Boolean, UniqueConstraint, BigInteger, inspect
        )
        hook = PostgresHook("destination_db")
        db_conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        flats_price = Table(
            "clean_flats_price",
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column("building_id", Integer),
            Column("floor", Integer),
            Column("kitchen_area", Float),
            Column("living_area", Float),
            Column("rooms", Integer),
            Column("is_apartment", Boolean),
            Column("studio", Boolean),
            Column("total_area", Float),
            Column("price", BigInteger),

            Column("build_year", Integer),
            Column("building_type_int", Integer),
            Column("latitude", Float),
            Column("longitude", Float),
            Column("ceiling_height", Float),
            Column("flats_count", Integer),
            Column("floors_total", Integer),
            Column("has_elevator", Boolean),
            Column("target", BigInteger),
            UniqueConstraint("id", name = "unique_clean_id_constraint")
        )
        if not inspect(db_conn).has_table(flats_price.name):
            metadata.create_all(db_conn)
    
    @task()
    def extract(**kwargs) -> pd.DataFrame: 
        hook = PostgresHook("destination_db")
        conn = hook.get_conn()
        sql = '''
            select  * from flats_price
        '''
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data = remove_duplicates(data)
        data = remove_missing_values(data)
        data = remove_outliers(data)
        return data
    
    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook("destination_db")
        hook.insert_rows(
            table="clean_flats_price",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
        )

    table_created = create_table()
    data = extract()
    transformed_data = transform(data)
    loaded_data = load(transformed_data)
    table_created >> loaded_data

clean_flats_price_dataset()