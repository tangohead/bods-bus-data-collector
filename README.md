# BODS Collector

A small tool to help collect data from the [UK Government's Bus Open Data Service (BODS)](https://data.bus-data.dft.gov.uk/). Currently, this tool repeatedly grabs the latest location information from the BODS Location API for all buses of a given operator.

The tool has two modes:
1. Save each update to a JSON file (e.g. for hosting), overwriting each time.
2. Save each update to a PostgreSQL database.

## Requirements
* Python 3.6+ 
* Docker

Note that the guide below refers to Linux. This should run fine on Windows but I haven't tested it. Some commands such as activating the virtual environment will change a little.

## Usage

```
usage: bus_data_downloader.py [-h] [--db] [--aws]
                              [--aws_filename AWS_FILENAME]
                              [--sleep_interval SLEEP_INTERVAL]
                              operator_code output_path

Tool to collect and publish the latest BODS data for a given operator.

positional arguments:
  operator_code         The BODS operator code to grab.
  output_path           Location to save each update to.

optional arguments:
  -h, --help            show this help message and exit
  --db                  Save each update to a database. (default: False)
  --aws                 Push to S3 Bucket on each update. (default: False)
  --aws_filename AWS_FILENAME
                        Name to push to S3 bucket. (default:
                        current_bus_locations.json)
  --sleep_interval SLEEP_INTERVAL
                        How many seconds to sleep between each pull from the
                        API. (default: 15)
```

## Setup

To use this tool, you will need a BODS API key. To get one, sign up to BODS [here](https://data.bus-data.dft.gov.uk/).

You will also need to set up `credentials.py`, plus `.env` and `db.env` if you want to use PostgreSQL.

### Setting up your virtual environment

As always, it's best to set up a virtual environment. After changing to this repo, run:
```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

Note that if you install on MacOS, you may encounter an issue building Psycopg2 - if so, you can install OpenSSL with Homebrew and build as follows:
```
env LDFLAGS="-I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib" pip install psycopg2
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
### Push to AWS

To push to AWS, simply set up your AWS credentials using the [AWS CLI tool](https://aws.amazon.com/cli/).
### Credentials.py

Use the template [credentials.py.tmpl](credentials.py.tmpl) and fill in your BODS API key.

If you are using Postgres, also fill in your database details, making sure that they are in quotes and match the ones defined in the environment files above.

If you want to push to an S3 bucket, then make sure to set your bucket name.
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

To push to AWS:
```
python3 bus_data_downloader.py [OPERATOR CODE] [JSON_PATH] --aws
```