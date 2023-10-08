import csv
import os
import uuid
from datetime import datetime
from io import StringIO
from typing import Union

import boto3


def lambda_handler(event, context) -> dict:
    data = event['detail']
    write_s3 = WriteS3()
    write_s3.send_data_for_s3(data)
    response = {
        "status": 200,
        "response": "write in s3 created with success"
    }
    return response


class WriteS3:
    def _init_(self) -> None:
        self.__event_bridge_client = boto3.client("events")
        self.__s3_client = boto3.client("s3")
        self.__bucket_name = os.environ["BUCKET_NAME_OFFERS"]

    def send_data_for_s3(self, data: dict) -> None:
        for record in data["Records"]:
            adm = record['adm']
            path_adm = self.write_in_csv(record, adm)

    def write_in_csv(self, data_item: list, name_adm: str) -> Union[None, str]:
        name_file = f"{name_adm}-{uuid.uuid4()}.csv"
        if not data_item:
            return None
        header = data_item[0].keys()
        csv_content = self.create_csv_string(data_item, header)
        return self.write_in_s3(csv_content, name_file)

    @staticmethod
    def create_csv_string(data_item: list, header: str) -> str:
        csv_string = StringIO()
        write_csv = csv.DictWriter(csv_string, fieldnames=header)
        write_csv.writeheader()
        for data in data_item:
            write_csv.writerow(data)
        csv_content = csv_string.getvalue()
        csv_string.close()
        return csv_content

    def write_in_s3(self, csv_content, name_file: str) -> str:
        date_now = datetime.now()
        date_format = date_now.strftime("%Y-%m-%d")
        path_s3 = f"{date_format}/{name_file}"
        self.__s3_client.put_object(
            Body=csv_content.encode("utf-8"), Bucket=self.__bucket_name, Key=path_s3
        )
        return path_s3
