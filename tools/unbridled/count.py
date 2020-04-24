#! ../../bin/python -w
import os

import boto3
import botocore
import time
from boto3.resources.base import ServiceResource  # noqa
from boto3.dynamodb.conditions import Key, Attr
from boto3.exceptions import Boto3Error
from botocore.exceptions import ClientError

class DDBResource:
    def __init__(self, **kwargs):
        conf = kwargs
        if not conf.get("endpoint_url"):
            if os.getenv("AWS_LOCAL_DYNAMODB"):
                conf.update(dict(
                    endpoint_url=os.getenv("AWS_LOCAL_DYNAMODB"),
                    aws_access_key_id="Bogus",
                    aws_secret_access_key="Bogus"
                ))
        if "endpoint_url" in conf and not conf["endpoint_url"]:
            del(conf["endpoint_url"])
        region = conf.get(
            "region_name",
            os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        if "region_name" in conf:
            del(conf["region_name"])
        self.conf = conf
        self._resource = boto3.resource(
            "dynamodb",
            config=botocore.config.Config(region_name=region),
            **self.conf
        )

    def __getattr__(self, name):
        return getattr(self._resource, name)

    def get_latest_message_tablenames(self, prefix="message", previous=1):
        # type: (Optional[str], int) -> [str]  # noqa
        """Fetches the name of the last message table"""
        client = self._resource.meta.client
        paginator = client.get_paginator("list_tables")
        tables = []
        for table in paginator.paginate().search(
                "TableNames[?contains(@,'{}')==`true`]|sort(@)[-1]".format(
                    prefix)):
            if table and table.encode().startswith(prefix):
                tables.append(table)
        if not len(tables) or tables[0] is None:
            return [prefix]
        tables.sort()
        return tables[0-previous:]

    def get_latest_message_tablename(self, prefix="message"):
        # type: (Optional[str]) -> str  # noqa
        """Fetches the name of the last message table"""
        return self.get_latest_message_tablenames(
                prefix=prefix,
                previous=1
            )[0]


# Get the list of "recent" users from router table these are desktop users that have
# connected in the last 30 days.
def recent_users(resource, start_time, router_table_name="router"):
    table = resource.Table(router_table_name)
    scan_start = time.strftime("%Y%m%d0000", start_time)
    print(scan_start)
    filter_ex = Attr('last_connect').gte(int(scan_start)) & Attr('router_type').eq('webpush')
    response = table.scan(
        FilterExpression=filter_ex,
        ProjectionExpression="uaid"
    )
    return map(lambda x: x['uaid'], response['Items'])



# iterate through "recent" users to discount any that have channelids in the message
# table.

def has_chids(uaid, table):
    key_ex = Key('uaid').eq(uaid) & Key('chidmessageid').eq(' ')
    filter_ex = Attr('channel_ids').not_exists()
    response = table.query(
        KeyConditionExpression=key_ex,
        FilterExpression=filter_ex
        )
    return response['Count'] > 0

# return count

def tick(count):
    if count % 1000 == 0:
        print("|")
    elif count % 100 == 0:
        print(".")

def main():
    # days = 30
    days = 1046
    message_table_name = "message"
    resource = boto3.resource('dynamodb')
    start_time = time.gmtime(time.time() - (days * 86400))
    message_table = resource.Table(message_table_name)
    print ("Gathering users:")
    total_users = recent_users(resource=resource, start_time=start_time)
    unbridled = []
    print ("Checking {} users:", len(total_users))
    count = 0
    for user in total_users:
        tick(count)
        count += 1
        if has_chids(uaid=user, table=message_table):
            unbridled.append(user)
            print(user)

    print("Total: {} \nUnbridled: {}\n".format(len(total_users), len(unbridled)))

if __name__ == '__main__':
    main()