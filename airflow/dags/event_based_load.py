from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.auth.transport.requests import Request
from google.oauth2 import service_account

import pandas as pd
import requests
import yaml
import boto3
import os
import time

from urllib.parse import quote
from datetime import datetime
import re
import unicodedata


def normalize_arabic(text):
    """
    Normalize Arabic text:
    - Unify alef variants
    - Remove diacritics
    - Normalize taa marbuta
    """

    if pd.isna(text):
        return text

    text = str(text)

    # Remove Arabic diacritics
    text = re.sub(
        r'[\u0617-\u061A\u064B-\u0652]',
        '',
        text
    )

    # Normalize alef variants
    text = text.replace("أ", "ا")
    text = text.replace("إ", "ا")
    text = text.replace("آ", "ا")

    # Normalize taa marbuta
    text = text.replace("ة", "ه")

    # Remove extra spaces
    text = " ".join(text.split())

    return text


def load_config():
    with open("/opt/airflow/config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def extract_incremental():

    print("🔵 Incremental Extract started")

    config = load_config()

    spreadsheet_id = config["google"]["spreadsheet_id"]
    service_account_file = config["google"]["service_account_file"]
    sheets_to_skip = config["google"]["sheets_to_skip"]

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly"
    ]

    credentials = service_account.Credentials.from_service_account_file(
        f"/opt/airflow/config/{service_account_file}",
        scopes=scopes
    )

    credentials.refresh(Request())

    headers = {
        "Authorization": f"Bearer {credentials.token}"
    }

    # Read Airflow Variable
    watermarks = Variable.get(
        "sheets_row_watermarks",
        default_var={},
        deserialize_json=True
    )

    # Get spreadsheet metadata
    meta_url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"

    meta = requests.get(
        meta_url,
        headers=headers
    ).json()

    final_data = []

    for sheet in meta["sheets"]:

        title = sheet["properties"]["title"]

        if title in sheets_to_skip:
            continue

        print(f"📄 Processing {title}")

        # ----------------------------
        # Read header row (A1:Z1)
        # ----------------------------
        encoded_title = quote(title, safe="")

        header_url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/"
            f"{spreadsheet_id}/values/{encoded_title}!A1:Z1"
        )

        header_response = requests.get(
            header_url,
            headers=headers
        )

        header_values = header_response.json().get("values", [])

        if not header_values:
            continue

        headers_row = header_values[0]

        # ----------------------------
        # Read only new rows
        # ----------------------------
        last_row = watermarks.get(title, 1)

        range_name = f"{encoded_title}!A{last_row + 1}:Z"

        data_url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/"
            f"{spreadsheet_id}/values/{range_name}"
        )

        response = requests.get(
            data_url,
            headers=headers
        )

        values = response.json().get("values", [])

        if not values:
            print(f"✅ No new rows in {title}")
            continue

        for row in values:

            row_dict = dict(zip(headers_row, row))

            row_dict["sheet_name"] = title

            final_data.append(row_dict)

        # Update watermark
        watermarks[title] = last_row + len(values)

    df = pd.DataFrame(final_data)

    os.makedirs(
        "/opt/airflow/data",
        exist_ok=True
    )

    df.to_csv(
        "/opt/airflow/data/event_extract.csv",
        index=False
    )

    Variable.set(
        "sheets_row_watermarks",
        watermarks,
        serialize_json=True
    )

    print(f"✅ New rows extracted: {len(df)}")



def transform_incremental():

    print("🟡 Transforming incremental data...")

    df = pd.read_csv(
        "/opt/airflow/data/event_extract.csv"
    )

    df.columns = df.columns.str.strip()

    # Normalize Arabic text columns
    arabic_columns = [
    "name",
    "grade",
    "location",
    "data_source",
    "data_source_1",
    "data_source_2"
]

    for col in arabic_columns:
        if col in df.columns:
           df[col] = df[col].apply(normalize_arabic)

    required_columns = ["name", "mobile", "grade"]

    df = df.dropna(
        subset=[col for col in required_columns if col in df.columns]
    )

    # drop unknown grade
    if "grade" in df.columns:
        df = df[df["grade"] != "غير معرف"]

    # normalize mobile
    if "mobile" in df.columns:

        def clean_mobile(m):

            if pd.isna(m):
                return m

            m = str(m)

            if "/" in m:
                m = m.split("/")[0]

            if len(m) == 9 and m.startswith(("77","78","79")):
               return "962" + m

            return m


        df["mobile"] = df["mobile"].apply(clean_mobile)


    # grade conversion
    if "grade" in df.columns:
        df["grade"] = df["grade"].replace(
            {"عاشر": "2010"}
        )


    # add timestamp
    df["timestamp"] = datetime.utcnow().isoformat()


    # add missing columns
    if "id" not in df.columns:
        df.insert(
            0,
            "id",
            range(1, len(df)+1)
        )


    required_schema_columns = [
        "location",
        "data_source",
        "data_source_2",
        "data_source_1",
        "upload_date"
    ]


    for col in required_schema_columns:
        if col not in df.columns:
            df[col] = None


    df = df[
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


    df.to_parquet(
        "/opt/airflow/data/event.parquet",
        engine="pyarrow",
        index=False
    )


    print(
        f"✅ Transform done. Rows: {len(df)}"
    )


def load_incremental():

    config = load_config()


    s3 = boto3.client(
        "s3",
        aws_access_key_id=config["aws"]["access_key_id"],
        aws_secret_access_key=config["aws"]["secret_access_key"],
        region_name=config["aws"]["region"]
    )


    timestamp = datetime.utcnow().strftime(
        "%Y%m%d_%H%M%S"
    )


    path = (
        f"full_load/{datetime.utcnow():%Y/%m/%d}/"
        f"event_{timestamp}.parquet"
    )


    s3.upload_file(
        "/opt/airflow/data/event.parquet",
        config["s3"]["bucket"],
        path
    )


    print(
        f"✅ Uploaded {path}"
    )


default_args = {
    "owner": "airflow"
}
with DAG(
    dag_id="event_based_load",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False
) as dag:


    t1 = PythonOperator(
        task_id="extract_incremental",
        python_callable=extract_incremental
    )


    t2 = PythonOperator(
        task_id="transform_incremental",
        python_callable=transform_incremental
    )


    t3 = PythonOperator(
        task_id="load_incremental",
        python_callable=load_incremental
    )


    t1 >> t2 >> t3