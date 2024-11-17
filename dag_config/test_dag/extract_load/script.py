import requests
import sys
import base64
import zipfile
import os
import pickle


def main(execution_date, airflow_connection, airflow_variable):
    # Get link download from variable Airflow, please fill from link data needed.
    url = airflow_variable['link_download_hospital_patient_records']
    airflow_home_path = airflow_variable['airflow_home_path']
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        filename = airflow_home_path + "/hospital_patient_records.zip"
        # Open a local file to write the content
        with open(filename, "wb") as file:
            file.write(response.content)
        print("File " + filename + " downloaded successfully.")

        # Extract the ZIP file
        folder = airflow_home_path + "/data/hospital_patient_records/" + execution_date
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            # Create the folder to extract files into
            if not os.path.exists(folder):
                os.makedirs(folder)
            zip_ref.extractall(folder)

        # Delete zip file after extact
        if os.path.exists(filename):
            os.remove(filename)
    else:
        print(f"Failed to download file. HTTP Status Code: {response.status_code}")


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
