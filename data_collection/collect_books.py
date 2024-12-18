import requests
from typing import Dict, Union
from datetime import date, datetime
from argparse import ArgumentParser
from utils import save_data
import pandas as pd

BASE_API_URL = "https://openlibrary.org/works/"
FILE_FORMAT = ".json"


def collect_single_book_data(
    base_url: str, open_library_id: str, file_format: str
) -> Dict:
    url = f"{base_url}{open_library_id}{file_format}"
    try:
        r = requests.get(url)

        if r.status_code == 200:
            return r.json()
        else:
            msg = (
                f"and error occurred in the request. It returned code: {r.status_code}"
            )
            raise Exception(msg)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    parser = ArgumentParser(description="Parser of book collection")
    parser.add_argument(
        "--data_lake_path", required=True, help="Airflow's data lake path in docker"
    )
    parser.add_argument(
        "--open_library_ids", required=True, help="List of books to be downloaded"
    )
    parser.add_argument(
        "--execution_date", required=True, help="Execution date of the Airflow DAG"
    )
    args = parser.parse_args()
    IDS_LIST = args.open_library_ids.split(",")
    blocklist = []

    try:
        df = pd.read_parquet(
            f"{args.data_lake_path}refined/parquet/books/books.parquet"
        )
        df["collect_date"] = pd.to_datetime(df["collect_date"])
        df["execution_date"] = args.execution_date
        df["execution_date"] = pd.to_datetime(df["execution_date"])
        df["diff_months"] = df["execution_date"].dt.to_period("M").astype(int) - df[
            "collect_date"
        ].dt.to_period("M").astype(int)

        blocklist = df.query("diff_months > 12")["id"].values.tolist()
    except FileNotFoundError:
        pass

    cleaned_ids_list = list(set(IDS_LIST) - set(blocklist))

    for item in cleaned_ids_list:
        item = item.strip()
        dt = datetime.strptime(args.execution_date, "%Y-%m-%d").strftime("%Y%m%d")
        file_name = f"{dt}_book_{item}"
        response_json = collect_single_book_data(BASE_API_URL, item, FILE_FORMAT)
        save_data(
            response_json,
            file_name,
            context="books",
            file_type="json",
            base_path=args.data_lake_path,
        )