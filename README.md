# BODS Collector

A small tool to help collect data from the [UK Government's Bus Open Data Service (BODS)](https://data.bus-data.dft.gov.uk/). Currently, this tool repeatedly grabs the latest location information from the BODS Location API for all buses of a given operator.

The tool has two modes:
1. Save each update to a JSON file (e.g. for hosting), overwriting each time.
2. Save each update to a PostgreSQL database.

## Requirements
* Python 3.6+ 
* Docker

## Setup

To use this tool, you will need a BODS API key. To get one, sign up to BODS [here](https://data.bus-data.dft.gov.uk/).

You will also need to set up `credentials.py`, plus `.env` and `db.env` if you want to use PostgreSQL.

### Setting up your virtual environment

As always, it's best to set up a virtual environment. After changing to this repo, run:
```
python3 -m venv venv
pip3 install -r requirements.txt
```

### Setting up PostgreSQL

You can skip this section if you only want to output JSON.

If you want to use your own, already hosted Postgres database, just fill in [credentials.py.tmpl](credentials.py.tmpl) with the username, password, host and port.

If you want to use a Docker-hosted Postgres database, fill in [.env.tmpl](.env.tmpl) and [db.env.tmpl](db.env.tmpl) to make `.env` and `db.env` files.

`.env`:
```
LOCAL_PORT=the port you want to expose locally for the database
LOCAL_PATH=the path you want to store the data in, or just a name such as pgdata.
```
`db.env`:
```
POSTGRES_USER=the database username (pick what you want!)
POSTGRES_DB=the database name (pick what you want!)
POSTGRES_PASSWORD=the database password (make it good!)
```

Once you have set these files up, run:
```
docker-compose up -d
```
This will set up your database.

### Credentials.py

Use the template [credentials.py.tmpl](credentials.py.tmpl) and fill in your BODS API key.

If you are using Postgres, also fill in your database details, making sure that they are in quotes and match the ones defined in the environment files above.

### Setting up the Database

Again, skip this if you only want to output JSON.

Next we need to create the table for storing the data. To do this, run:
```
python3 bus_data_models.py
```
This will connect to the database and set up the required table.

## Running the Tool

You will need to find the operator code for the operator you want to collect data on. You can find these on the [Traveline NOC Database](https://www.travelinedata.org.uk/traveline-open-data/transport-operations/browse/).

To run just in JSON mode:
```
python3 bus_data_downloader.py [OPERATOR CODE] [JSON_PATH]
```

To run in DB mode too:
```
python3 bus_data_downloader.py [OPERATOR CODE] [JSON_PATH] --db
```


