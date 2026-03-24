import boto3

client = boto3.client('dynamodb')

TABLE_NAME = 'silver-dev'
PK_VALUE = '8ea6d439-768f-4b3e-887a-498ff159555f'

response = client.query(
    TableName=TABLE_NAME,
    KeyConditionExpression= 'PK = :pk AND begins_with(SK, :sk)',
    ExpressionAttributeValues={
        ':pk': {'S': PK_VALUE},
        ':sk': {'S': 'INVOICE'}
    }
)

print(response['Items'])
"""
[{
  'PK': {'S': '559808a3-bf16-493f-9c4f-ada1f27e140a'},
  'SK': {'S': 'INVOICE#1234567890'},
  'sourcePath': {'S': 'path/invoice/2023-03-24/nome_do_arquivo.avro'},
  'item1': {'S': 'ABC123'},
  'status': {'S': 'OK'},
  'item2': {'L': [{'S': 'ABC'}]},
  'datetime': {'S': '2025-06-13 16:55:21.313770'},
},
{
...
}]
"""

print(len(response['Items']))