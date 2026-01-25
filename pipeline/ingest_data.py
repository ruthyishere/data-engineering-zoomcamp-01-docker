#!/usr/bin/env python
# coding: utf-8

import pandas as pd

from sqlalchemy import create_engine

from tqdm.auto import tqdm

import click # to get params from cli 




dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

# @click.option('--year', help='Year of the data to ingest', default=2021, type=int)
# @click.option('--month', help='Month of the data to ingest', default=1, type=int)

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command() # allows us to run the function from the command line
@click.option('--filepath', help='Path to the CSV file to ingest', required=True)
@click.option('--pg-user', help='Postgres username', default='root')
@click.option('--pg-pass', help='Postgres password', default='root')
@click.option('--pg-host', help='Postgres host', default='localhost')
@click.option('--pg-port', help='Postgres port', default='5432')
@click.option('--pg-db', help='Postgres database name', default='ny_taxi')
@click.option('--chunksize', help='Number of rows to process at a time', default=100000, type=int)
@click.option('--tablename', help='Name of the table to write data to', default='yellow_taxi_data')

def run (filepath, pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, tablename):
    # pg_user = "root"
    # pg_password = "root"
    # pg_host = "localhost"
    # pg_db = "ny_taxi"
    # pg_port = "5432"

    # year = 2021
    # month = 1

    # tablename = "yellow_taxi_data"

    # chunksize = 100000

    # prefix + 'yellow_tripdata_2021-01.csv.gz'

    #prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    #url = f'{prefix}yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Read first row to get column names
    df_sample = pd.read_csv(filepath, nrows=0)
    cols = df_sample.columns.tolist()
    
    # Filter dtype and parse_dates to only include columns that exist
    valid_dtype = {k: v for k, v in dtype.items() if k in cols}
    valid_parse_dates = [d for d in parse_dates if d in cols]

    df_iter = pd.read_csv(
        filepath,
        dtype=valid_dtype,
        parse_dates=valid_parse_dates,
        iterator=True,
        chunksize=chunksize,
    )
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=tablename, con=engine, if_exists='replace')
            first = False
        df_chunk.to_sql(name=tablename, con=engine, if_exists='append')

if __name__ == '__main__':
    run()