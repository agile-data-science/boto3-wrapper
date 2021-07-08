import ast

import pandas as pd
import boto3
from botocore.exceptions import ClientError


class S3Selecter:
    def __init__(self, bucket):
        self.bucket = bucket
        self.chunk_bytes = 524288  # 512KB or 0.5MB
        self.s3_client = boto3.client("s3")

    def _get_s3_file_size(self, key) -> int:
        """Gets the file size of S3 object by a HEAD request

        Args:
            key (str): S3 object path

        Returns:
            int: File size in bytes. Defaults to 0 if any error.
        """
        # aws_profile = current_app.config.get('AWS_PROFILE_NAME')
        # s3_client = boto3.session.Session(profile_name=aws_profile).client('s3')

        file_size = 0
        try:
            response = self.s3_client.head_object(Bucket=self.bucket, Key=key)
            if response:
                file_size = int(
                    response.get("ResponseMetadata")
                    .get("HTTPHeaders")
                    .get("content-length")
                )
        except ClientError:
            Exception(f"Client error reading S3 file {self.bucket} : {key}")
        return file_size

    def _stream_s3_file_json(self, key, expression, file_size: int):
        """Streams a S3 file via a generator.

        Args:
            expression (str): Query
            file_size (int):
        Returns:
            Generator: Returns a tuple of dict
        """
        # aws_profile = current_app.config.get('AWS_PROFILE_NAME')
        # s3_client = boto3.session.Session(profile_name=aws_profile).client('s3')
        start_range = 0
        end_range = min(self.chunk_bytes, file_size)

        while start_range < file_size:
            response = self.s3_client.select_object_content(
                Bucket=self.bucket,
                Key=key,
                ExpressionType="SQL",
                Expression=expression,
                InputSerialization={
                    "CSV": {
                        "FileHeaderInfo": "Use",
                        "FieldDelimiter": ",",
                        "RecordDelimiter": "\n",
                    }
                },
                OutputSerialization={"JSON": {"RecordDelimiter": ","}},
                ScanRange={"Start": start_range, "End": end_range},
            )

            """
            select_object_content() response is an event stream that can be looped to concatenate the overall result set
            Hence, we are joining the results of the stream in a string before converting it to a tuple of dict
            """
            result_stream = []

            for event in response["Payload"]:
                if records := event.get("Records"):
                    result_stream.append(
                        # records["Payload"].decode("utf-8", errors="ignore")
                        records["Payload"]
                    )

            # HACK: decodeはこっちで
            if len(result_stream) > 0:
                yield ast.literal_eval(b"".join(result_stream).decode("utf-8"))

            start_range = end_range
            end_range = end_range + min(self.chunk_bytes, file_size - end_range)

    def get_dataset(self, key, expression):
        try:
            file_size = self._get_s3_file_size(key)
            items = ()
            for i in self._stream_s3_file_json(key, expression, file_size):
                items += i

            df = pd.DataFrame([item.values() for item in items]).replace('"', "")
            df.columns = items[0].keys()
        except:
            df = pd.DataFrame()
        return df


import json
from io import StringIO


class S3Reader:
    def __init__(self, bucket):
        self.bucket = bucket
        self.s3_client = boto3.client("s3")

    def read_csv(self, file_path, **kwargs):
        try:
            key = file_path
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            body = obj["Body"].read()
            bodystr = body.decode("utf-8")
            df = pd.read_csv(StringIO(bodystr), **kwargs)
        except ClientError:
            # HACK: raise推奨
            df = pd.DataFrame()
        return df

    def read_json(self, file_path):
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
        body = obj['Body'].read()
        bodystr = body.decode('utf-8')
        dict_data = json.loads(bodystr)
        return dict_data

    def put_df_as_csv(self, file_path, df: pd.DataFrame):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

        response = self.s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=self.bucket,
            Key=file_path
        )
        return response

    def get_file_or_folder_list(self, path, f_type):
        if f_type == "folder":
            path = path + '/'
            folder_name_list = []
            folder_obj = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=path, Delimiter='/')
            print("folder_obj", folder_obj)
            for folder_name in folder_obj.get('CommonPrefixes'):
                name = folder_name.get('Prefix')
                folder_name_list.append(name)

            folder_name_list = list(
                map(lambda x: x.rstrip('/'), folder_name_list))
            folder_name_list = list(
                map(lambda x: x.split('/')[-1], folder_name_list))

            return folder_name_list

        elif f_type == "file":
            file_name_list = []
            file_obj = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=path)
            if file_obj.get('Contents') == None:
                return []
            print("file_obj", file_obj)
            for file_name in file_obj.get('Contents'):
                name = file_name['Key']
                file_name_list.append(name)

            file_name_list = list(
                map(lambda x: x.split('/')[-1], file_name_list))

            return file_name_list
