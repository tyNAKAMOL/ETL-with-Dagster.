import pandas as pd
from dagster import asset

@asset
def load(transform):
    # df = pd.read_csv('/opt/airflow/data/stagingFile.csv')
    df = transform
    df.to_csv("C:/Users/beaut/Documents/Dagster/ETL/ETL-project/ETL_project/data/finalFile.csv", index=False) 