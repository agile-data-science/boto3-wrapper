import boto3
from boto3.dynamodb.conditions import Key


class Table:
    """[summary]"""

    def __init__(self, table_name, region_name="ap-northeast-1"):
        self.table = self._define_table(table_name, region_name)

    def _define_table(self, table_name, region_name):
        dynamodb_resource = boto3.resource("dynamodb", region_name=region_name)
        return dynamodb_resource.Table(table_name)

    def _to_list(self, generator):
        return [item for item in generator]

    def scan(self, **kwargs):
        result = self._recursive_response("scan", **kwargs)
        return self._to_list(result)

    def get_with_GSI(self, GSI, key, value):
        result = self._recursive_response(
            "query", IndexName=GSI, KeyConditionExpression=Key(key).eq(value)
        )
        return self._to_list(result)

    def get(self, keyEx: Key):
        """
        KeyEx = Key(key).eq(value)
        KeyEx = KeyEx & Key(sort_key).eq(sort_value)
        """
        result = self._recursive_response("query", KeyConditionExpression=keyEx)
        return self._to_list(result)

    def delete(self, key_dict: dict):
        """
        key_dict = {
            'primary_key': 'value',
            'sort_key': 'value'}
        """

        response = self.table.delete_item(Key=key_dict)
        return response

    def put(self, item):
        table = self.table
        response = table.put_item(Item=item)
        return response

    def update(self, key_dict, update_key, update_value):
        """
        key_dict = {
            'primary_key': 'value',
            'sort_key': 'value'}
        """
        response = self.table.update_item(
            Key=key_dict,
            UpdateExpression=f"set {update_key}=:{update_key}",
            ExpressionAttributeValues={f":{update_key}": update_value},
        )
        return response

    def _response(self, type, **kwargs):
        if type == "scan":
            response = self.table.scan(**kwargs)
        elif type == "query":
            response = self.table.query(**kwargs)
        else:
            raise ValueError("type must be scan or query.")
        return response

    def _recursive_response(self, type, **kwargs):
        while True:
            response = self._response(type, **kwargs)
            for item in response["Items"]:
                yield item
            if "LastEvaluatedKey" not in response:
                break
            kwargs.update(ExclusiveStartKey=response["LastEvaluatedKey"])


#     def _convert_float_to_decimal(self, float_dict_data):
#         return json.loads(json.dumps(float_dict_data), parse_float=decimal.Decimal)

#     def _convert_decimal_to_float(self, decimal_dict_data):
#         return json.loads(json.dumps(decimal_dict_data, default=_decimal_to_int_or_float))


# def _decimal_to_int_or_float(obj):
#     if isinstance(obj, Decimal):
#         if obj == int(obj):
#             return int(obj)
#         else:
#             return float(obj)
#     raise TypeError
