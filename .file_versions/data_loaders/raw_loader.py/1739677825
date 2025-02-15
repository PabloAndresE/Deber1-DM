from mage_ai.settings.repo import get_repo_path
import pandas as pd
import snowflake.connector
import os
import yaml

# Cargar credenciales desde YAML
def load_snowflake_credentials():
    yaml_path = os.path.join(get_repo_path(), 'snowflake.yaml')
    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['default']


# BLOQUE 1: Data Loader
@data_loader
def load_data(*args, **kwargs):
    config_path = os.path.join(get_repo_path(), 'snowflake.yaml')


    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    conn = snowflake.connector.connect(
    user="Pablo",
    password="hwship17TRN312",
    account="xqb21060.us-east-1",
    database="INSTACART_DB",  # Reemplázalo con el correcto
    schema="RAW"
    )

    tables = ['ORDERS', 'PRODUCTS', 'AISLES', 'DEPARTMENTS', 'ORDER_PRODUCTS']
    data = {}

    for table in tables:
        print(f"Cargando {table} desde Snowflake...")
        query = f"SELECT * FROM RAW.{table}"
        
        # Ejecutar la consulta correctamente con pd.read_sql
        df = pd.read_sql(query, conn)  
        data[table] = df

        print(f"Tabla {table} cargada exitosamente.")



    conn.close()  # Cerrar la conexión después de cargar los datos
    return data
