# Import what is needed
import datetime
import base64
import pickle
from airflow import DAG
from airflow import configuration as airflow_cfg
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow import settings
from airflow.models import Connection, Variable


task_config = {task_config}
list_of_task_have_upstreams = {list_of_task_have_upstreams}
list_upstream_task = {list_upstream_task}
execution_date = {execution_date}
dag_params = {dag_params}

# Create a DAG object
dag = DAG(
    {dag_id},
    **dag_params
)

# Get all connections and variable needed
session = settings.Session()

# Query all connections
all_connection = session.query(Connection).all()

connections = dict()
for connection in all_connection:
    push_conn = dict(
        conn_type=connection.conn_type,
        host=connection.host,
        schema=connection.schema,
        username=connection.login,
        password=connection.password,
        login=connection.login,
        port=connection.port,
        extra=connection.get_extra()
    )
    connections[connection.conn_id] = push_conn

# Query all variable
all_variable = session.query(Variable).all()
variables = dict()
for variable in all_variable:
    variables[variable.key] = variable.val

env = dict(
    execution_date=execution_date,
    connections=base64.b64encode(pickle.dumps(connections)),
    variables=base64.b64encode(pickle.dumps(variables))
)

# Generate task
tasks = dict()
for task in task_config:
    task_id = task['task_id']
    file_path = task['file_path']

    # Define task
    tasks[task_id] = BashOperator(
        task_id=task_id,
        bash_command="python3 " + airflow_cfg.get_airflow_home() + "/" + file_path + " $execution_date $connections $variables",
        env=env,
        dag=dag
    )

    # Handle external_sensors
    if 'external_sensors' in task:
        external_sensors = []
        for sensor in task['external_sensors']:
            # Loop external_sensors task
            for task_wait in sensor['external_task_id']:
                task_sensor = ExternalTaskSensor(
                    task_id="sensor_" + task_wait + "_" + task_id,
                    external_task_id=task_wait,
                    external_dag_id=sensor['external_dag_id'],
                    allowed_states=['success', 'skipped'],
                    mode='reschedule',
                    timeout=sensor['timeout'],
                    poke_interval=sensor['poke_interval'],
                    execution_delta=datetime.timedelta(minutes=sensor['execution_delta']),
                    dag=dag,
                    retries=sensor['retries']
                )
                external_sensors.append(task_sensor)

        external_sensors >> tasks[task_id]

    # Handle task without task_upstreams
    if ('task_upstreams' not in task and task_id not in list_upstream_task):
        tasks[task_id]

# Mapping downtream task to upstream
if list_of_task_have_upstreams:
    for task_upstream in list_of_task_have_upstreams:

        upstreams = []
        for upstream in task_upstream['upstreams']:
            upstreams.append(tasks[upstream])

        tasks[task_upstream['id']].set_upstream(upstreams)
        tasks[task_upstream['id']]
