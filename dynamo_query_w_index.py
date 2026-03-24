import boto3


dynamodb = boto3.client('dynamodb')
s3 = boto3.resource('s3')

def get_data_dynamo(client, entity):
    query_kwargs = {
        'TableName': 'silver-prod',
        'IndexName': 'GSI-clientId-receivedAt',
        'KeyConditionExpression': f'clientId = :clientId AND receivedAt >= :start',
        'FilterExpression': 'begins_with(SK, :sk)',
        'ExpressionAttributeValues': {
            ':clientId': {'S': client},
            ':start': {'S': '2023-01-01'},
            ':sk': {'S': entity}
        },
        'ReturnConsumedCapacity': 'TOTAL'
    }
    return dynamodb.get_paginator('query').paginate(**query_kwargs)


client = '559808a3-bf16-493f-9c4f-ada1f27e140a'
entity = 'ITEM#'
pages_data = get_data_dynamo(client, entity)

for page in pages_data:
    print(page['Items'])