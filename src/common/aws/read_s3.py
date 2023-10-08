import boto3


class ReadS3:
    def _init_(self) -> None:
        self.__client_s3 = boto3.client("s3")

    def read_from_s3(self, path_s3: str, bucket_name: str) -> list:
        response = self.__client_s3.get_object(Bucket=bucket_name, Key=path_s3)
        return response["Body"].read().decode("utf-8")
