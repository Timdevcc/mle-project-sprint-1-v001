from airflow.decorators import dag, task
from notifications.message import send_telegram_failure_message, send_telegram_success_message
import pendulum


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    catchup=False,
    tags=['ETL'],
    max_active_runs=1,
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def prepare_flats_price_dataset():
    import numpy as np
    import pandas as pd
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
            "flats_price",
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
            Column("price", Float),

            Column("build_year", Integer),
            Column("building_type_int", Integer),
            Column("latitude", Float),
            Column("longitude", Float),
            Column("ceiling_height", Float),
            Column("flats_count", Integer),
            Column("floors_total", Integer),
            Column("has_elevator", Boolean),
            UniqueConstraint("id", name = "unique_id_constraint")
        )
        if not inspect(db_conn).has_table(flats_price.name):
            metadata.create_all(db_conn)

    @task()
    def extract(**kwargs) -> pd.DataFrame: 
        hook = PostgresHook("destination_db")
        conn = hook.get_conn()
        sql = '''
            select f.*,
            b.build_year,
            b.building_type_int,
            b.ceiling_height,
            b.latitude,
            b.longitude,
            b.flats_count,
            b.floors_total,
            b.has_elevator
            from flats as f
            left join buildings as b on f.building_id = b.id
        '''
        data = pd.read_sql(sql, conn)
        conn.close()
        return data
    
    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        return data.copy()
    
    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flats_price",
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

prepare_flats_price_dataset()