from datetime import datetime
from airflow.decorators import dag 
from airflow.operators.python import PythonOperator


import polars as pl

import glob
import pandas as pd
import geopandas as gpd
from polars._typing import SchemaDict
import polars_st as st


from scripts.transformation import order_columns_by_schema, add_location_ids, col_rename, values_map, add_missing_columns, enforce_schema_types



DIR = r"C:\Users\stgadmin\Desktop\Taxi_AirFlow\data\test-in\yellow\2025\*.parquet"
files = glob.glob(DIR)
target_schema_2025_dict = pl.scan_parquet(files[0]).collect_schema()

DIR = r"C:\Users\stgadmin\Desktop\Taxi_AirFlow\data\test-in\yellow\**\*.parquet"
files = glob.glob(DIR)
SHAPEFILE_PATH = r"C:\Users\stgadmin\Desktop\Taxi_AirFlow\data\taxi_zones\\"
zones_gdf = gpd.read_file(SHAPEFILE_PATH)
zones_df = st.from_geopandas(zones_gdf.to_crs("EPSG:4326"))
zones_lazy = zones_df.select(['geometry', 'LocationID']).lazy()

def run_transformation(
    source_lazy_df: pl.LazyFrame,
    zones_df: pl.DataFrame,
    target_schema: SchemaDict
) -> pl.LazyFrame:
    """
        renvoie Un nouveau LazyFrame aligné sur le schéma cible.
    """
    print("--- START ---")

   
    df_step1 = col_rename(source_lazy_df)

    
    df_step2 = add_location_ids(df_step1, zones_df)

    
    df_step3 = values_map(df_step2)

    
    df_step4 = add_missing_columns(df_step3, target_schema)

    
    df_step5 = enforce_schema_types(df_step4, target_schema)
    
    
    final_lazy_df = order_columns_by_schema(df_step5, target_schema)

    print("--- END ---")
    
    return final_lazy_df

def run_all(files): 
    for file in files : 
        data = pl.scan_parquet(file)
        file_name = file
        file_again = file.split('\\')
        file_again[6] = 'test_out'
        file_name = ('\\').join(file_again)
        print(file_name)
        result = run_transformation(data,zones_lazy,target_schema_2025_dict)
        result.sink_parquet(file_name) 

@dag(
    dag_id ="test_taxi",
    schedule="@daily",
    start_date = datetime(2025,9,18),
    tags=["taxi"] 
)
def generate_dag():
    task1 = PythonOperator( 
        task_id = "transformation",
        python_callable=run_all,
        op_kwargs={"files": files}
    )
    

    task1


generate_dag()


