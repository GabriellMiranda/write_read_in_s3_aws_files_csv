import os
from io import StringIO
import pandas as pd

from src.common.aws.read_s3 import ReadS3


def lambda_handler(event, context) -> dict:
    data = event['detail']
    path_name = data['path_name']
    bucket_name = os.environ['BUCKET_NAME']
    read_s3 = ReadS3()
    file = read_s3.read_from_s3(path_name, bucket_name)
    data_file = process_csv_string(file)
    response = {
        "status": 200,
        "response": data_file
    }
    return response


def process_csv_string(csv_content) -> list:
    df = pd.read_csv(StringIO(csv_content))
    return df.to_dict(orient='records')
