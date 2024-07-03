import os
import sys
from dotenv import load_dotenv
from google.cloud import bigquery
from dagster import job, op, schedule
from utils.gsheets_connection import read_gsheets
from utils.create_dataset_id import new_dataset_id
from utils.clean_functions import parse_dates, format_numeric_values
from utils.all_queries import queries


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)
load_dotenv()
GSHEETS_URL = "https://docs.google.com/spreadsheets/d/1uLo02a8HceETDrC50Pzjt41hOOVtNC6EvYeMAc6GnkA/edit?gid=0#gid=0"
FILE_PATH = os.path.join(PROJECT_ROOT, os.getenv("CREDENTIALS_PATH"))
client = bigquery.Client.from_service_account_json(FILE_PATH)


@op
def load_data_from_sheets(context):
    dict_df = {}
    sheets = [
        "categorias",
        "pedidos",
        "empleados",
        "clientes",
        "pedidos_detalles",
        "productos",
        "transportistas",
    ]

    for sheet in sheets:
        dict_df[sheet] = read_gsheets(GSHEETS_URL, [sheet])

    context.log.info("Data loaded")
    return dict_df


@op
def clean_data(context, dict_df):
    dict_df["pedidos"] = parse_dates(
        dict_df["pedidos"], ["fechaPedido", "fechaRequerida", "fechaEnvio"]
    )
    numeric_columns = {
        "pedidos_detalles": ["precioUnitario", "cantidad", "descuento"],
        "productos": ["precioUnitario"],
        "pedidos": ["flete"],
    }
    for table_name, cols in numeric_columns.items():
        dict_df[table_name] = format_numeric_values(dict_df[table_name], cols)
    context.log.info("Data transformed")
    return dict_df


@op
def create_new_dataset(context):
    dataset_id = "papyrus-technical-test.prueba_Cristhian"
    new_dataset_id(dataset_id)
    context.log.info("New dataset created")
    return dataset_id


@op
def load_data_to_bigquery(context, dict_df, dataset_id):
    for table_name, df in dict_df.items():
        table_id = f"{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        context.log.info(f"Data loaded to BigQuery table {table_name}")


@op
def run_queries_and_create_tables(context):
    dataset_id = "papyrus-technical-test.prueba_Cristhian"
    for table_name, query in queries.items():
        table_id = f"{dataset_id}.{table_name}"
        query_job = client.query(query)
        results = query_job.result().to_dataframe()
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        client.load_table_from_dataframe(
            results, table_id, job_config=job_config
        ).result()
        context.log.info(f"Table {table_name} created with results")


@job
def full_commerce_pipeline():
    dict_df = load_data_from_sheets()
    dict_df = clean_data(dict_df)
    dataset_id = create_new_dataset()
    load_data_to_bigquery(dict_df, dataset_id)
    run_queries_and_create_tables()


@schedule(cron_schedule="0 */2 * * *", job=full_commerce_pipeline)
def full_commerce_schedule(_context):
    return {}
