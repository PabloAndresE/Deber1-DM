from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_snowflake(data, **kwargs):
    """
    Exporta todas las tablas desde MySQL a Snowflake, creando la DB y el esquema si no existen.
    """
    config_path = path.join(get_repo_path(), 'snowflake.yaml')
    config_profile = 'default'

    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as snowflake_loader:
        # Crear base de datos y esquema en Snowflake si no existen
        snowflake_loader.execute("CREATE DATABASE IF NOT EXISTS INSTACART_DB;")
        snowflake_loader.execute("CREATE SCHEMA IF NOT EXISTS INSTACART_DB.RAW;")

        for table, df in data.items():
            print(f"Exportando {table} a Snowflake...")

            # Exportar cada DataFrame a Snowflake
            snowflake_loader.export(
                df,
                database="INSTACART_DB",
                schema="RAW",
                table_name=table.upper(),  # Convertimos el nombre a mayúsculas para Snowflake
                if_exists="replace"  # Opciones: 'replace', 'append', 'fail'
            )

            print(f"Tabla {table} exportada.")

    print("Todas las tablas fueron exportadas a Snowflake")