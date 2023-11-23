# Monthly active users per organization in ZITADEL

## Prerequisites

- Python 3.x
- Service User PAT with Manager role IAM_OWNER_VIEWER

## Install

Create a file called `.env` with the following contents.

```text
PAT="D1...qQ"
BASE_URL="abc.zitadel.cloud"
START_YEAR=2023
```

Create the output folder.

`mkdir ./output`

Create and activate virtual environment.

```bash
python3 -m venv venv
source venv/bin/activate
```

Install dependencies.

`pip install -r requirements.txt`

Generate the ZITADEL client including dependencies (takes a couple of seconds).
This will create several package folders.

`buf generate https://github.com/zitadel/zitadel#format=git --include-imports`

## Run

Activate the environment.

`python3 -m venv venv`

Download the data from ZITADEL.
Output will be saved as pickle (pkl) file in /output.

`python3 main.py`

Transform the data and save the output as csv file in /output.
You can also use the Notebook (.ipynb) instead.

`python3 mau_per_org.py`

## Debug

If something breaks with the connection to ZITADEL use the following flags to get more information about the grpc request.

`GRPC_VERBOSITY=DEBUG GRPC_TRACE=http python3 main`