from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_from_mysql(*args, **kwargs):
    """
    Extrae todas las tablas de MySQL y devuelve un diccionario con DataFrames.
    """
    tables = ["aisles", "departments", "orders", "products", "order_products"]
    
    config_path = path.join(get_repo_path(), 'mysql.yaml')
    config_profile = 'default'

    data_dict = {}

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for table in tables:
            print(f"Cargando {table} desde MySQL...")
            query = f"SELECT * FROM {table};"
            data_dict[table] = loader.load(query)
            print(f"Tabla {table} cargada.")

    return data_dict
