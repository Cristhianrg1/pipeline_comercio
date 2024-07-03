# Comercio Data Pipeline

Este proyecto contiene un pipeline de datos para cargar, transformar y consultar datos desde hojas de Google Sheets y almacenarlos en BigQuery.

## Estructura del Proyecto

- `dags/dag_comercio.py`: Contiene el código principal del pipeline utilizando Dagster.
- `dags/utils/all_queries.py`: Contiene las consultas SQL utilizadas para crear las tablas en BigQuery.
- `dags/utils/clean_functions.py`: Contiene funciones de limpieza de datos para los dataframes.
- `dags/utils/create_dataset_id.py`: Contiene función que permite crear un nuevo dataset_id en Bigquery.
- `dags/utils/gsheets_connection.py`: Contiene función que permite consultar información en un google sheets.
- `.env`: Archivo para configurar las variables de entorno (no incluido en el repositorio).
- `requirements.txt`: Archivo con todas las dependencias necesarias para el proyecto.
- `credentials.json`: Archivo con las credenciales para acceder a Bigquery. Debe ser agregado en la raíz del proyecto.

## Instalación

1. Crear entorno virtual:
   ```bash
   python3 -m venv dagster-env
   ```

2. Activar el entorno virtual:
   ```bash
   source dagster-env/bin/activate
   ```

3. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```
4. Crear un archivo `.env` en la raíz del proyecto con el siguiente contenido:
   ```plaintext
   CREDENTIALS_PATH=credentials.json
   ```

## Ejecución

1. Inicializar Dagit
   ```bash
   dagit -f dags/dag_comercio.py
   ```
2. Acceder a la interfaz gráfica
   ```bash
   http://localhost:3000
   ```
