from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
try:
    from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
except ImportError:
    from airflow.providers.amazon.aws.hooks.dynamodb import AWSDynamoDBHook as DynamoDBHook
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import secrets
import requests


# Define default_args for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 9, 7),
    'retries': 1,
}

vechile_pos = 'api/V1/Gtfs/Feed/VehiclePosition'
KEY = Variable.get("go_api_key")

# Create a DAG instance
dag = DAG(
    'go_train_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # Set the schedule interval as needed
    catchup=False,  # Set to True if you want historical DAG runs
    max_active_runs=1,  # Ensure only one DAG run is active at a time
    tags=['example'],
)

def push_to_dynamodb(data, table):
    dynamodb = DynamoDBHook(table_name=table)
    data = json.loads(json.dumps(data), parse_float=Decimal)
    dynamodb.write_batch_data(data)

# Define the Python function to print "Hello, World"
def create_execution_id():
    return f'{int(datetime.utcnow().timestamp())}-{secrets.token_hex(4)}'


def read_xcom(**kwargs):
    ti = kwargs['ti']

    res = ti.xcom_pull(task_ids='get_go_data')
    # Create a list to store filtered train data
    parsed = json.loads(res)
    train_data = []
    #logging.info(parsed)

    missing = []

    # Iterate through the "entity" list and filter for train data
    for entity in parsed["entity"]:
        label = entity["vehicle"]["vehicle"]["label"]
        # Check if the label starts with an alphabet letter
        if label == '':
            missing.append(entity)
            continue

        if label[0].isalpha():
            train_data.append(entity)
    #print(train_data)

    to_upload = [
        {
            'id': entry['vehicle']['vehicle']["id"],
            'ts': entry['vehicle']['timestamp'],
            'lat': entry['vehicle']['position']['latitude'],
            'long': entry['vehicle']['position']['longitude'],
            'route': entry['vehicle']['vehicle']["label"]
        }
        for entry in train_data
    ]
    logging.info(len(to_upload))
    logging.info(len(missing))
    push_to_dynamodb(to_upload, 'go_trains')

def fetch_json_data(url):
    json_data = []
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Parse the JSON data from the response
            json_data = response.json()
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
    return json_data

def process_via_rail_data(data):
    active_trains = []
    for key in data.keys():
        if data[key].get('arrived') == False:
            if not data[key].get('poll'):
                continue
            active_trains.append({
                'ts': float(datetime.fromisoformat(data[key]['poll'].replace('Z', '+00:00')).replace(tzinfo=timezone.utc).timestamp()),
                'id': key,
                'lat': data[key]['lat'],
                'long': data[key]['lng'],
                'from': data[key]['from'],
                'to': data[key]['to']
                })
    return(active_trains)



def via_data():
    url = 'https://tsimobile.viarail.ca/data/allData.json'
    data = fetch_json_data(url)
    via_trains = process_via_rail_data(data)
    push_to_dynamodb(via_trains, 'via_trains')


get_via_data_task = PythonOperator(
    task_id='via_data',
    python_callable=via_data,
    provide_context=True,
    dag=dag,
)


get_go_data_task = SimpleHttpOperator(
  task_id="get_go_data", 
  http_conn_id='GO_API', 
  method="GET", 
  endpoint=f"{vechile_pos}?key={KEY}", 
  do_xcom_push=True,
  dag=dag
)

process_go_data_task = PythonOperator(
    task_id='process_go_data',
    python_callable=read_xcom,
    provide_context=True,
    dag=dag,
)

# Create a PythonOperator that will execute the print_hello_world function
id_task = PythonOperator(
    task_id='execution_id',
    python_callable=create_execution_id,
    dag=dag,
)

# Define the task dependencies
id_task >> get_go_data_task >> process_go_data_task
id_task >> get_via_data_task
