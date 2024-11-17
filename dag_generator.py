"""DAG generator script."""
import yaml
import os
from pathlib import Path
from datetime import timedelta


# File base dag template
file_base = "dag_template.py"

# Loop File config.yml in Folder dag_config
dag_list = Path('dag_config').glob('*/config.yml')
for dag in dag_list:
    dag = str(dag)
    path_dag = str(Path(dag).parent)

    # Load yaml Config File
    config = open(dag, 'r')
    dag_params = yaml.load(config, Loader=yaml.FullLoader)
    config.close()

    if 'default_args' in dag_params and\
            'execution_timeout' in dag_params['default_args']:
        dag_params['default_args']['execution_timeout'] = \
            timedelta(
                seconds=dag_params['default_args']['execution_timeout'])

    dag_id = path_dag.split("/")[-1]
    list_of_task_have_upstreams = []
    list_upstream_task = []

    # Load file script.py to crafting task of DAG
    task_list = Path(path_dag).glob('*/script.py')
    task_config = []
    for task in task_list:
        task = str(task)
        path_task = str(Path(task).parent)
        path_task_yml = task.replace('script.py', 'config.yml')
        task_id = path_task.split("/")[-1]

        if os.path.exists(path_task_yml):
            task_config_yml = open(path_task_yml, 'r')
            task_params = yaml.load(task_config_yml, Loader=yaml.FullLoader)
            task_config_yml.close()
        else:
            task_params = {}

        task_params['task_id'] = task_id
        task_params['file_path'] = task

        # external_sensors parameter, have default value per parameter.
        if 'external_sensors' in task_params:
            external_sensors = []
            for sensor in task_params['external_sensors']:
                sensor['timeout'] = \
                    (task_params['external_sensors']['timeout']
                        if 'timeout' in task_params['external_sensors']
                        else 3600)

                sensor['poke_interval'] = \
                    (task_params['external_sensors']['poke_interval']
                        if 'poke_interval' in task_params['external_sensors']
                        else 300)

                sensor['retries'] = \
                    (task_params['external_sensors']['retries']
                        if 'retries' in task_params['external_sensors']
                        else 1)

                sensor['execution_delta'] = \
                    (task_params['external_sensors']['execution_delta']
                        if 'execution_delta' in task_params['external_sensors']
                        else 0)

                external_sensors.append(sensor)
            task_params['external_sensors'] = external_sensors

        # task_upstreams parameter.
        if 'task_upstreams' in task_params:
            list_of_task_have_upstreams.append({
                'id': task_id,
                'upstreams': task_params['task_upstreams']
            })
            list_upstream_task.extend(task_params['task_upstreams'])

        task_config.append(task_params)

    # Create DAG based on dag_template.py
    file_open = open('dags/' + dag_id + '.py', 'w')
    with open(file_base, "r") as f:
        template = f.read()
        template = \
            template.format(
                dag_id="'" + dag_id + "'",
                execution_date="\"{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S') }}\"",
                dag_params=dag_params,
                task_config=task_config,
                list_of_task_have_upstreams=list_of_task_have_upstreams,
                list_upstream_task=list_upstream_task)

        file_open.write(template)
        file_open.close()
