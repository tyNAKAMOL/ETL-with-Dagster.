import pandas as pd
import re
from dagster import asset,Output

@asset
def transform(extract):
    # df = pd.read_csv('/opt/airflow/data/sourceFile.csv')
    df = extract

    #remove_special_chars
    df['First Name'] = df['First Name'].str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)
    df['Last Name'] = df['Last Name'].str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)

    #remove whitespace
    df['First Name'] = df['First Name'].str.replace(" ",'')
    df['Last Name'] = df['Last Name'].str.replace(" ",'')
    df['Sex'] = df['Sex'].str.replace(" ",'')

    #The first character is uppercase
    df['First Name'] = df['First Name'].str.title()
    df['Last Name'] = df['Last Name'].str.title()

    # drop Age Error
    df_drop = df[pd.to_numeric(df['Age'], errors='coerce').notnull()]
    df_drop["Age"] = df_drop['Age'].astype(int)

    #Sex
    df_drop['Sex'] = df_drop['Sex'].str.upper()
    df_drop['Sex'] = df_drop['Sex'].str.replace("(MAN|MALE)",'M',regex=True)
    df_drop['Sex'] = df_drop['Sex'].str.replace("(GIRL|FEM)",'F',regex=True)
    df_drop['Sex'] = df_drop['Sex'].str.replace("(BOTH|FM|MF)",'LGBT',regex=True)
    df_drop['Sex'] = df_drop['Sex'].str.replace("-",'Not Defined',regex=True)

    #counting
    age_morethan120 = int((df_drop['Age']>120).sum())
    sex_LGBT = int(df_drop['Sex'].str.count('LGBT').sum())
    sex_NotDefined = int(df_drop['Sex'].str.count('NOT DEFINED').sum())

    # # display on dragster
    # obj1={}
    # obj1 = {
    # "age_morethan120":age_morethan120,
    # "sex_LGBT":sex_LGBT,
    # "sex_NotDefined":sex_NotDefined  
    # }

    df_drop.to_csv("C:/Users/beaut/Documents/Dagster/ETL/ETL-project/ETL_project/data/stagingFile.csv", index=False) 

    return Output(  # The return value is updated to wrap it in `Output` class
        value=df_drop,  # The original df is passed in with the `value` parameter
        metadata={
            "age_morethan120":str(age_morethan120),
            "sex_LGBT":sex_LGBT,
            "sex_NotDefined":sex_NotDefined 
        },
    )