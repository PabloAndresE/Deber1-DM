import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

BATCH_SIZE = 50000

@transformer
def transform_clean_data(data, *args, **kwargs):
    print("Iniciando procesamiento de datos por lotes...")

    transformed_data = {}

    fact_orders_batches = []
    for start in range(0, len(data['ORDER_PRODUCTS']), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = data['ORDER_PRODUCTS'].iloc[start:end].copy()
        if 'days_since_prior_order' in batch.columns:
            batch["days_since_prior_order"].fillna(batch["days_since_prior_order"].median(), inplace=True)
        batch.rename(columns={"add_to_cart_order": "order"}, inplace=True)
        fact_orders_batches.append(batch)
    
    transformed_data["fact_orders"] = pd.concat(fact_orders_batches, ignore_index=True)
    
    transformed_data["dim_products"] = data['PRODUCTS'][['product_id', 'product_name', 'aisle_id', 'department_id']]
    transformed_data["dim_departments"] = data['DEPARTMENTS'][['department_id', 'department']].rename(columns={'department': 'department_name'})
    transformed_data["dim_aisles"] = data['AISLES'][['aisle_id', 'aisle']].rename(columns={'aisle': 'aisle_name'})

    dim_orders_batches = []
    for start in range(0, len(data['ORDERS']), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = data['ORDERS'].iloc[start:end].copy()
        batch.drop_duplicates(inplace=True)
        dim_orders_batches.append(batch)

    transformed_data["dim_orders"] = pd.concat(dim_orders_batches, ignore_index=True)

    transformed_data["dim_users"] = transformed_data["dim_orders"][['user_id']]

    print("Proceso de transformación por lotes completado. Tablas generadas:", list(transformed_data.keys()))
    
    return transformed_data
