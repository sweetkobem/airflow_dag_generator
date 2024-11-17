import sys
import base64
import pickle
import duckdb
from datetime import datetime


def main(execution_date, airflow_connection, airflow_variable):
    # Create variable airflow_home_path and fill path of root/home path airflow
    airflow_home_path = airflow_variable['airflow_home_path']

    # Load data from CSV
    path_ = airflow_home_path + '/data/hospital_patient_records/' + execution_date
    patients = duckdb.query(f"SELECT * FROM '{path_}/patients.csv'")
    procedures = duckdb.query(f"SELECT * FROM '{path_}/procedures.csv'")
    encounters = duckdb.query(f"SELECT * FROM '{path_}/encounters.csv'")
    payers = duckdb.query(f"SELECT * FROM '{path_}/payers.csv'")

    avg_pasient_day = duckdb.query("""
            SELECT
                round(AVG(total_pasient), 2) AS value
            FROM (SELECT
            start::DATE AS date,
            COUNT(DISTINCT patient) as total_pasient
            FROM procedures
            GROUP BY 1)
        """)

    patient_return_day = duckdb.query("""
            SELECT
                round(AVG(datediff('day', prev_date, start)), 2) AS value
            FROM (SELECT
            patient,
            start,
            LAG(stop) OVER (PARTITION BY patient ORDER BY stop ASC) AS prev_date
            FROM procedures)
            WHERE prev_date IS NOT NULL
        """)

    avg_duration_hour_stay = duckdb.query("""
            SELECT
            round(AVG(datediff('hour', start, stop))) AS value
            FROM encounters
        """)

    cost_avg_cost_per_visit = duckdb.query("""
            SELECT
            round(AVG(total_claim_cost), 2) AS value
            FROM encounters
        """)

    procedure_cover_by_insurance = duckdb.query("""
            SELECT
            COUNT(encounters.id) AS value
            FROM encounters
            LEFT JOIN payers
                ON encounters.payer = payers.id
            WHERE name = 'NO_INSURANCE'
        """)

    date = execution_date.split(" ")[0]
    merge_data = duckdb.query(f"""
            SELECT
                '{date}' AS date,
                'How avg pasient in day ?' AS question,
                value AS answer FROM avg_pasient_day
            UNION ALL
            SELECT
                '{date}' AS date,
                'How avg in day patient return to hospital ?',
                value FROM patient_return_day
            UNION ALL
            SELECT
                '{date}' AS date,
                'How long are patients staying in the hospital on average hour ?',
                value FROM avg_duration_hour_stay
            UNION ALL
            SELECT
                '{date}' AS date,
                'How much is the average cost per visit ?',
                value FROM cost_avg_cost_per_visit
            UNION ALL
            SELECT
                '{date}' AS date,
                'How many procedures are covered by insurance ?',
                value FROM procedure_cover_by_insurance
        """)
    merge_data_df = merge_data.to_df()

    # Store data to persistence storage DuckDB
    con = duckdb.connect(database="my-db.duckdb", read_only=False)
    con.execute("CREATE OR REPLACE TABLE sample_analysis AS SELECT * FROM merge_data_df")

    # Show result from persistence storage DuckDB
    result = con.sql("SELECT * FROM sample_analysis")
    print(result)
    con.close()


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
