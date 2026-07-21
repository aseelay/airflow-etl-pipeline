from airflow import DAG
from airflow.operators.python import PythonOperator

from google.oauth2 import service_account
from google.auth.transport.requests import Request

import pandas as pd
import yaml
import requests
import boto3
import os
import time

from datetime import datetime
from urllib.parse import quote


# =====================
# Config
# =====================

def load_config():
    with open(
        "/opt/airflow/config/config.yaml",
        "r",
        encoding="utf-8"
    ) as f:
        return yaml.safe_load(f)



# =====================
# Google Auth
# =====================

def create_credentials():

    config = load_config()

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly"
    ]

    credentials = service_account.Credentials.from_service_account_file(
        f"/opt/airflow/config/{config['google']['service_account_file']}",
        scopes=scopes
    )

    credentials.refresh(Request())

    return credentials



def get_headers(credentials):

    return {
        "Authorization": f"Bearer {credentials.token}"
    }



# =====================
# Extract
# =====================

def extract():

    print("🔵 FULL LOAD EXTRACT START")

    config = load_config()

    spreadsheet_id = config["google"]["spreadsheet_id"]
    sheets_to_skip = config["google"]["sheets_to_skip"]


    credentials = create_credentials()

    headers = get_headers(credentials)


    # get sheets list

    meta_url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/"
        f"{spreadsheet_id}"
    )


    response = requests.get(
        meta_url,
        headers=headers
    )


    print("META STATUS:", response.status_code)


    sheets = response.json()["sheets"]


    final_rows = []


    for sheet in sheets:

        sheet_name = sheet["properties"]["title"]


        if sheet_name in sheets_to_skip:
            print("Skipping:", sheet_name)
            continue


        print(
            "📥 Reading:",
            sheet_name
        )


        encoded = quote(
            sheet_name,
            safe=""
        )


        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/"
            f"{spreadsheet_id}/values/{encoded}"
        )


        success = False


        for i in range(5):

            response = requests.get(
                url,
                headers=headers
            )


            if response.status_code == 200:
                success=True
                break


            elif response.status_code == 401:

                credentials.refresh(Request())
                headers = get_headers(credentials)


            elif response.status_code == 429:

                time.sleep(20)


        if not success:
            print(
                "❌ Failed:",
                sheet_name
            )
            continue



        data = response.json().get(
            "values",
            []
        )


        if len(data)<2:
            continue


        columns=data[0]


        for row in data[1:]:

            row_dict=dict(
                zip(columns,row)
            )

            row_dict["sheet_name"]=sheet_name

            final_rows.append(row_dict)



    df=pd.DataFrame(final_rows)


    os.makedirs(
        "/opt/airflow/data",
        exist_ok=True
    )


    df.to_csv(
        "/opt/airflow/data/extract.csv",
        index=False
    )


    print(
        "✅ Extract rows:",
        len(df)
    )



# =====================
# Transform
# =====================

def transform():

    print("🟡 TRANSFORM START")


    df=pd.read_csv(
        "/opt/airflow/data/extract.csv"
    )


    df.columns=df.columns.str.strip()


    # remove unknown grade

    if "grade" in df.columns:

        df=df[
            df["grade"]!="غير معرف"
        ]

        df["grade"]=df["grade"].replace(
            {
                "عاشر":"2010"
            }
        )


    # mobile clean

    if "mobile" in df.columns:


        def clean_mobile(x):

            x=str(x)


            if "/" in x:
                x=x.split("/")[0]


            if (
                len(x)==9
                and (
                    x.startswith("77")
                    or x.startswith("78")
                    or x.startswith("79")
                )
            ):
                x="962"+x


            return x


        df["mobile"]=df["mobile"].apply(
            clean_mobile
        )


    df.insert(
        0,
        "id",
        range(1,len(df)+1)
    )


    df["timestamp"]=datetime.utcnow().isoformat()


    for col in [
        "location",
        "data_source",
        "data_source_1",
        "data_source_2",
        "upload_date"
    ]:

        if col not in df.columns:
            df[col]=None



    df=df[
        [
        "id",
        "name",
        "mobile",
        "grade",
        "location",
        "data_source",
        "data_source_2",
        "data_source_1",
        "sheet_name",
        "upload_date",
        "timestamp"
        ]
    ]


    df.to_csv(
        "/opt/airflow/data/transform.csv",
        index=False
    )


    print(
        "✅ Transform rows:",
        len(df)
    )



# =====================
# Load S3
# =====================

def load():

    print("🟢 LOAD START")


    config=load_config()


    df=pd.read_csv(
        "/opt/airflow/data/transform.csv"
    )


    parquet="/opt/airflow/data/event_data.parquet"


    df.to_parquet(
        parquet,
        engine="pyarrow",
        index=False
    )


    s3=boto3.client(
        "s3",
        aws_access_key_id=config["aws"]["access_key_id"],
        aws_secret_access_key=config["aws"]["secret_access_key"],
        region_name=config["aws"]["region"]
    )


    s3.upload_file(
        parquet,
        config["s3"]["bucket"],
        config["s3"]["full_load_path"]
    )


    print(
        "✅ Uploaded FULL LOAD"
    )



# =====================
# DAG
# =====================

with DAG(
    dag_id="full_excel_load",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False
) as dag:


    extract_task=PythonOperator(
        task_id="extract",
        python_callable=extract
    )


    transform_task=PythonOperator(
        task_id="transform",
        python_callable=transform
    )


    load_task=PythonOperator(
        task_id="load",
        python_callable=load
    )


    extract_task >> transform_task >> load_task