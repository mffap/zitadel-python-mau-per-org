import grpc
import logging, sys
import os
import pandas as pd
import datetime

from dotenv import load_dotenv

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp

import zitadel.admin_pb2_grpc as adminService
import zitadel.management_pb2_grpc as managementService
from zitadel.admin_pb2 import ListEventsRequest, ListOrgsRequest
from zitadel.management_pb2 import ListUsersRequest

load_dotenv()

logging.basicConfig(level=logging.DEBUG)

class ZitadelClient(object): 

    def __init__(self, token, base_url, port="443"):

        if not token: 
            raise ValueError("Admin client requires an access token")

        self.host = base_url
        self.server_port = port

        scc = grpc.ssl_channel_credentials()
        tok = grpc.access_token_call_credentials(token)
        ccc = grpc.composite_channel_credentials(scc, tok)
        url = '{}:{}'.format(self.host, self.server_port)

        # instantiate a channel
        self.channel = grpc.secure_channel(url, ccc)

        # bind the client and the server
        self.AdminService = adminService.AdminServiceStub(self.channel)
        self.ManagementService = managementService.ManagementServiceStub(self.channel)

        logging.debug("Created zitadel client connected to {}".format(url))

def timestamp_from_datetime(dt):
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts

base_url = os.environ["BASE_URL"]
token = os.environ["PAT"]
year = int(os.environ["START_YEAR"])

client = ZitadelClient(token, base_url)

dt = datetime.datetime(year, 1, 1, 0,0,0,0, datetime.timezone.utc)
start_date = timestamp_from_datetime(dt)

logging.debug("Starting to get all user.token.added events")

tokens_per_user = []

def get_all_events(limit=None, event_types=[], start_date=None, end_date=None, asc=True):
    
    if limit is None: 
        limit = 1000
    most_recent_ts = start_date
    end_date = datetime.datetime.now(datetime.timezone.utc)
    has_more = True
    events = []

    while has_more:

        message = client.AdminService.ListEvents(ListEventsRequest(limit=limit, event_types=event_types, creation_date=most_recent_ts, asc=asc), wait_for_ready = True)
        event_page = MessageToDict(message)["events"]

        # most_recent = datetime.datetime.fromisoformat(event_page[-1]["creationDate"]) # Python 3.11 and later
        most_recent = datetime.datetime.fromisoformat(event_page[-1]["creationDate"].replace("Z", "+00:00")) # Before Python 3.11
        most_recent_ts = timestamp_from_datetime(most_recent)
        event_count = len(event_page)

        logging.debug("Limit: {}; Received: {}; Most recent: {}; End date: {}".format(limit, event_count, most_recent, end_date))

        events.extend(event_page)

        # Check if there's more
        if event_count < limit: 
            has_more = False

        if most_recent > end_date:
            has_more = False

    return events

all_events = get_all_events(limit=None,event_types=["user.token.added"], start_date=start_date)

for event in all_events: 
    tokens_per_user.append([event["aggregate"]["id"], event["creationDate"]])

logging.debug("Collected {} user.token.added events".format(len(all_events)))