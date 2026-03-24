import boto3
from dynamodb.query_wo_pagination import TABLE_NAME

client = boto3.client('dynamodb')
resource = boto3.resource('dynamodb')

def get_query_kwargs(TABLE_NAME, PK, SK):
    query_kwargs = {
        'TableName': TABLE_NAME,
        'KeyConditionExpression': 'PK = :pk AND begins_with(SK, :sk)',
        'ExpressionAttributeValues': {
            ':pk': {'S': PK},
            ':sk': {'S': SK}
        }
    }
    return query_kwargs

def get_paginate_dynamo_query(query_kwargs):
    page_iterator = client.get_paginator('query').paginate(**query_kwargs)
    return page_iterator

def run():
    TABLE_NAME = 'silver-dev'
    table = resource.Table(TABLE_NAME)

    id = '4f2ce7e0-a9e3-49a5-992e-59f945fb0564'
    entidade = 'INVOICE#'

    paginate_dynamo = get_paginate_dynamo_query(get_query_kwargs(TABLE_NAME, id, entidade))
    for page in paginate_dynamo:
        response = page['Items']
        for item in response:
            print(item)

            PK = item['PK']['S']
            SK = item['SK']['S']
            print(f"------{PK}: {SK}------")

            value = item['value']['S']
            new_value = "#" + value + "#"

            # UPDATE DYNAMO
            response = table.update_item(
                Key={
                    'PK': PK,
                    'SK': SK
                },
                UpdateExpression= "set value =:k",
                ExpressionAttributeValues={
                    ":k": new_value
                    }
                )