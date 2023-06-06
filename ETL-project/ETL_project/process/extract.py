import pandas as pd
import glob
from dagster import asset,Output, get_dagster_logger

@asset
def extract():
    logger = get_dagster_logger()
    folder_path = 'C:/Users/beaut/Documents/Dagster/ETL/ETL-project/ETL_project/data/source/'
    dataFrame = []
    first_file = True
    for file_path in glob.glob(f'{folder_path}raw*.csv'):
        if first_file is True:
            df = pd.read_csv(file_path,sep="|",header=0)
            first_file = False
        else:
            df = pd.read_csv(file_path,skiprows=0,sep="|",)
        dataFrame.append(df)
    if dataFrame != []:
        merged_df = pd.concat(dataFrame)
        merged_df.to_csv("C:/Users/beaut/Documents/Dagster/ETL/ETL-project/ETL_project/data/sourceFile.csv", index=False)
        return merged_df
    logger.info("Error")