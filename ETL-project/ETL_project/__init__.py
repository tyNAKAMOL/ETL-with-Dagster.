from dagster import Definitions, load_assets_from_modules

from ETL_project.process import extract,transform,load

all_assets = load_assets_from_modules([extract,transform,load])

defs = Definitions(
    assets=all_assets,
)
