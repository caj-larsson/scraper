import json
import boto3
import random
import urllib.parse
import uuid
import decimal
import time

from collections import defaultdict
from boto3.dynamodb.conditions import Key, Attr

# Local imports
import scraper
import mock_scraper
import config
import utils


def clean_dynamodb_result(value):
    """ Remove Decimal types and replace with Int for this project
    """
    if isinstance(value, decimal.Decimal):
        return int(value)
    elif isinstance(value, list):
        return [clean_dynamodb_result(v) for v in value]
    elif isinstance(value, dict):
        return {clean_dynamodb_result(k): clean_dynamodb_result(v)
                for k,v in value.items()}
    else:
        return value


dynamodb = boto3.resource("dynamodb")
sqs = boto3.resource("sqs", )


def send_to_connection(connection_id, connection_url, data):
    """ Send a message to the connected browser on the websocket.
    """
    gatewayapi = boto3.client(
        "apigatewaymanagementapi",
        endpoint_url=connection_url
    )

    return gatewayapi.post_to_connection(
        ConnectionId=connection_id,
        Data=json.dumps(data, indent=4, sort_keys=True).encode('utf-8')
    )


def filter_visited_urls(connection_id, links):
    """DynamoDB does not have any builtin way to do an anti join against a
    supplied set. So the options are to either query for each value we want to
    check. Or we fetch the all of them.

    I decided to go for the latter because one IO with some extra kB of data is
    nicer than hundredish sequential IO.

    FIXME: This turned out to be way to slow as there are thousands of
    pages with high connectivity. This has to be solved with a sorted
    composite index on f"{connection_id}|{referer_url}|{scrape_url}",
    at least. So that we can look up every page one by one.

    Further this the table structure should probably be changed. One
    data table for the page data and one for the connectivity graph
    arcs.

    """
    table = dynamodb.Table(config.RESULT_TABLE)

    response = table.scan(
        IndexName=config.RESULT_TABLE_CONNECTION_INDEX,
        ProjectionExpression="scrapeUrl",
        FilterExpression=Attr('connectionId').eq(connection_id)
    )

    visited_set = {d["scrapeUrl"] for d in response["Items"]}
    return list(set(links) - visited_set)


def write_scrape_segment(connection_id,
                         scrape_url,
                         referer,
                         title,
                         n_links,
                         n_images):
    """ Scrape segments will later be used to build a full map.
    """
    table = dynamodb.Table(config.RESULT_TABLE)
    response = table.put_item(
       Item={
            "Id": str(uuid.uuid4()),
            "connectionId": connection_id,
            "scrapeUrl": scrape_url,
            "referer": referer,
            "title": title,
            "n_links": n_links,
            "n_images": n_images
        }
    )


def update_job_counter(connection_id, difference):
    """ Update the atomic counter with a relative number of jobs finished.
    """
    table = dynamodb.Table(config.JOBS_TABLE)
    table.update_item(
        Key={
            "connectionId": connection_id
            },
        UpdateExpression="SET jobsLeft = jobsLeft + :difference",
        ExpressionAttributeValues={
            ":difference": difference
        }
    )


def get_jobs_left(connection_id):
    """ Simply ask for how many jobs remain in this task
    """
    table = dynamodb.Table(config.JOBS_TABLE)

    response = table.get_item(
        Key={
            "connectionId": connection_id
        }
    )
    return response["Item"]["jobsLeft"]


def build_hierarchy(pages):
    """ Constructs the hierarchial structure of pages, it will roll out a
    spanning tree of the graph of pages by recursively walking it.
    """
    referer_index = defaultdict(list)

    print(pages)
    for page in pages:
        referer_index[page["referer"]].append(page)

    # FIXME: needs to maintain the entire list of referers to prevent loops
    def build_recursive(referers):
        """Recursively link the pages up
        """
        # Most direct referer.
        referer = referers[-1]
        result = {}
        print(referer)

        for child in referer_index[referer]:

            new_child_node = child.copy()
            if child["scrapeUrl"] not in referers:
                # Break loops by not iterating into nodes that exist beneath this
                # point in the tree.
                new_child_node["links"] = build_recursive(referers + [child["scrapeUrl"]])

            # Remove redundant selflink
            del new_child_node["scrapeUrl"]
            # Remove redundant back link
            del new_child_node["referer"]

            result[child["scrapeUrl"]] = new_child_node
        return result

    return build_recursive([""])


def summary_and_exit(connection_id, connection_url):
    """ Generate the hiararchial data structure and send it.
    """
    table = dynamodb.Table(config.RESULT_TABLE)

    response = table.scan(
        IndexName="connectionId-index-copy",
        ProjectionExpression="n_images, n_links, title, referer, scrapeUrl",
        FilterExpression=Attr('connectionId').eq(connection_id)
    )
    items = response['Items']
    cleaned_items = clean_dynamodb_result(items)

    send_to_connection(
        connection_id,
        connection_url,
        build_hierarchy(cleaned_items)
    )


def job_invalid(connection_id):
    """ The job could be cancelled.
    """
    table = dynamodb.Table(config.JOBS_TABLE)
    response = table.get_item(Key={"connectionId": connection_id})

    return "Item" not in response


def queue_link_job(connection_id, connection_url, link, new_referer, max_depth):
    """ Spawn a child job
    """
    queue = sqs.get_queue_by_name(QueueName=config.JOBS_QUEUE)
    queue.send_message(MessageBody=json.dumps(
        {
            "connectionId": connection_id,
            "connectionUrl": connection_url,
            "scrapeUrl": link,
            "referer": new_referer,
            "maxDepth": max_depth
        })
    )


def build_job(connection_id, connection_url, link, new_referer, max_depth):
    return json.dumps(
        {
            "connectionId": connection_id,
            "connectionUrl": connection_url,
            "scrapeUrl": link,
            "maxDepth": max_depth
        }
    )


def run_job(job):
    connection_id = job["connectionId"]
    connection_url = job["connectionUrl"]
    scrape_url = job["scrapeUrl"]
    max_depth = job["maxDepth"]
    referer = job["referer"]

    if job_invalid(connection_id):
        return

    # Scrape and handle result
    if MOCK:
        n_images, n_links, links, title = mock_scrape(scrape_url)
    else:
       result = scraper.scrape_url(scrape_url)
       title = result.title
       n_images = result.n_images
       n_links = result.n_links
       links = result.local_links

    # Grab the most direct referer if there is one
    top_referer = referer[-1] if len(referer) > 0 else ""

    write_scrape_segment(
        connection_id,
        scrape_url,
        top_referer,
        title,
        n_links,
        n_images
    )

    send_to_connection(
        connection_id,
        connection_url,
        {
            "url": scrape_url,
            "n_images": n_images,
            "n_links": n_links,
            "title": "title"
        }
    )

    continue_recursion = max_depth > len(referer)

    # Ask dynamodb for the links that we have not already scraped
    unvisited_links = filter_visited_urls(connection_id, links)

    # Queue up jobs for links if we have not reached the end of the line
    if continue_recursion:
        new_referer = referer + [scrape_url]

        for link in unvisited_links:
            # Increase the remaining job counter
            update_job_counter(connection_id, 1)
            queue_link_job(
                connection_id,
                connection_url,
                link,
                new_referer,
                max_depth
            )
        # Since we queued new jobs we increase the diff

    # Reduce the remaining job counter.
    update_job_counter(connection_id, -1)
    # FIXME: Race condition here, should use the value read from the
    # atomic update. Can lead to missing that this was the last job
    # and thus missing to print structure.
    jobs_left = get_jobs_left(connection_id)

    if jobs_left <= 0:
        summary_and_exit(connection_id, connection_url)


def lambda_handler(event, context):
    """ Entrypoint for lambda
    """

    for record in event["Records"]:
       job = json.loads(record["body"])
       # At this point never want to rerun jobs on failure, but we
       # want to see the error.
       try:
          run_job(job)

       except Exception as e:
          print(utils.exception_to_string(e))

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
