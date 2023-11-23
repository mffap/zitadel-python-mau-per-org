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

events = MessageToDict(client.AdminService.ListEvents(ListEventsRequest(limit=None, event_types=["user.token.added"], creation_date=start_date, asc=True), wait_for_ready = True))

for event in events["events"]: 
    tokens_per_user.append([event["aggregate"]["id"], event["creationDate"]])

logging.debug("Collected {} user.token.added events".format(len(events["events"])))

users_per_org = []

organizations = MessageToDict(client.AdminService.ListOrgs(ListOrgsRequest()))

logging.debug("Found {} organizations".format(len(organizations["result"])))

for org in organizations["result"]:
    org_id = org["id"]
    org_name = org["name"]

    metadata = (('x-zitadel-orgid',org_id),)
    users = MessageToDict(client.ManagementService.ListUsers(request=ListUsersRequest(), metadata=metadata))
    for user in users["result"]: 
        users_per_org.append([user["id"], user["userName"], org_id, org_name])

logging.debug("Collected {} users across all organizations".format(len(users_per_org)))

df_tokens = pd.DataFrame(tokens_per_user, columns=["userId", "creationDate"])
df_users = pd.DataFrame(users_per_org, columns=["userId", "userName", "orgId", "orgName"])

df_tokens.to_pickle("output/tokens.pkl")
df_users.to_pickle("output/users.pkl")