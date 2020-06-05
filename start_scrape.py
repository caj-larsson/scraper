import json
import boto3

import config

dynamodb = boto3.resource("dynamodb")
sqs = boto3.resource("sqs")


def create_scrape_job(connetion_id, connection_url, scrape_url):
    table = dynamodb.Table(config.JOBS_TABLE)
    response = table.put_item(
       Item={
            "connectionId": connetion_id,
            "connectionUrl": connection_url,
            "scrapeUrl": scrape_url,
            "jobsLeft": 1
        }
    )

    return response


def enque_root_url_job(connection_id, connection_url, scrape_url, max_depth):
    queue = sqs.get_queue_by_name(QueueName=config.JOBS_QUEUE)
    queue.send_message(MessageBody=json.dumps(
        {
            "connectionId": connection_id,
            "connectionUrl": connection_url,
            "scrapeUrl": scrape_url,
            "referer": [],
            "maxDepth": max_depth
        })
    )


def lambda_handler(event, context):
    connection_id = event["requestContext"].get("connectionId")
    if not connection_id:
        raise ValueError("connectionId missing in requestContext")

    domain_name = event["requestContext"]["domainName"]
    stage = event["requestContext"]["stage"]
    connection_url = "https://" + domain_name + "/" + stage

    # If this needs to be dynamic we need to make state machine and read it from
    # the websocket. Significantly more work for a nice to have at this point.
    scrape_url = "https://www.pentesteracademy.com"

    create_scrape_job(connection_id, connection_url, scrape_url)

    enque_root_url_job(connection_id, connection_url, scrape_url, 2)

    return {
        'statusCode': 200,
        'body': "Ok"
    }
