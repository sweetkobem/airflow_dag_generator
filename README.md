# Airflow DAG Generator
Generator DAG for Airflow, to simplify the creation of DAGs using only a config.yml file and script.py, allowing individuals with limited knowledge of Airflow to easily create data pipelines, and then you can focus on building the data pipeline.

## Requirement: ##
- Apache Airflow >= 2
- DuckDB
- Yaml

## Step by step: ##
- Put dag_generator.py into root airflow folder.
- Create folder "dag_config" add "{dag_folder}" for configuration DAG.
- Create config.yml file for configuration DAG.
- Sub of folder DAG is for task, and inside folder task is script.py or config.yml to adding config like upstream, etc.
- After everything is set up, you can run dag_generator.py using 'python3 dag_generator.py', and voilà... the DAG defined in the config will be created in the Airflow DAG folder. 

## Example folder: ##
Example data using data from Mavenanalytics "Hospital Patient Records".
<br>Using [DuckDB](https://duckdb.org "DuckDB") for transformation: DuckDB is easy, fast, and I like it.

## Notes: ##
- All Airflow connections and variables can be utilized in your Python script (script.py) by following the provided sample code.
- config.yml fill DAG level parameter from Airflow, [here](https://www.astronomer.io/docs/learn/airflow-dag-parameters "here").
- config.yml in level task using for task_upstreams (list of task upstream) and external_sensors (list of external task want to sensor)
  ```
  external_sensors:
    timeout: 10
    poke_interval: 10,
    retries: 1,
    execution_delta: 120,
    external_dag_id: test_dag_2
    external_task_id:
      - ingest
  ```

## Preview Sample DAG: ##
<img width="1083" alt="Screenshot 2024-11-17 at 06 09 01" src="https://github.com/user-attachments/assets/6aba1b2b-88d2-4f2c-9bff-69b4ee4bad40">
<img width="488" alt="Screenshot 2024-11-17 at 06 09 16" src="https://github.com/user-attachments/assets/a2d4c024-ee66-48a4-bf0d-334df4aa9a2d">

You can article for this in medium [here](https://medium.com/@sweetkobem/creating-an-airflow-dag-generator-using-yaml-files-and-a-sample-transformation-with-duckdb-008b3f7c1e1d)
