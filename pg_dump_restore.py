import docker
import psycopg2

import subprocess
import requests
import json
import re
import sys
import base64
import zlib
import logging
import time

logging.basicConfig(level={
        'debug': logging.DEBUG,
        'warning': logging.WARNING,
        'info': logging.INFO
    }.get('info'))


def get_postgres_dump(access_token: str) -> str:
    """
    Get PostgreSQL dump from hackattic.com
    """
    url = f"https://hackattic.com/challenges/backup_restore/problem?access_token={access_token}"

    logging.info(f"Getting PostgreSQL dump from {url}")
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(
            f"Failed to get PostgreSQL dump from {url}"
            f"\n{response.text}"
        )
        sys.exit(1)
    dump = None
    try:
        dump = json.loads(response.text)
    except json.decoder.JSONDecodeError as e:
        logging.error(
            f"Failed to parse json output from {url}"
            f"\n{e}"
        )
        sys.exit(1)
    return dump['dump']


def decompress_and_decode_dump(dump: str) -> str:
    """
    Decompress and decode dump
    """
    logging.info("Decompressing and decoding dump")
    dump = zlib.decompress(base64.b64decode(dump), 16 + zlib.MAX_WBITS).decode('utf-8')
    return dump

def get_db_records(usr='postgres', pwd='postgres', hst='localhost', db='postgres', port=5432):
    """
    Get records from database

    By default - connecting to an exposed port on localhost and querying the db for results
    """
    try:
        connection = psycopg2.connect(user=usr,
                                      password=pwd,
                                      host=hst,
                                      port=port,
                                      database=db)
        cursor = connection.cursor()
        cursor.execute("select ssn from public.criminal_records where status='alive'")
        table_records = cursor.fetchall()

        return {'alive_ssns': [record[0] for record in table_records]}

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error while fetching data from PostgreSQL: {error}")

    finally:
        if connection:
            cursor.close()
            connection.close()
            logging.debug("PostgreSQL connection is closed")


def submit_solution(access_token, data):
    """
    Submit solution to hackattic.com
    """
    url = f"https://hackattic.com/challenges/backup_restore/solve?access_token={access_token}&playground=1"

    logging.info(f"Submitting solution to {url}")
    response = requests.post(url, data=json.dumps(data))
    if response.status_code != 200:
        logging.error(
            f"Failed to submit solution to {url}"
            f"\n{response.text}"
        )
        sys.exit(1)
    logging.debug(f"Solution submitted to {url}")
    return response.text


if __name__ == '__main__':

    username = 'postgres'
    db_name = 'postgres'
    access_token = sys.argv[1]

    dump = decompress_and_decode_dump(get_postgres_dump(access_token))
    db_version = re.findall(r'(?:Dumped from database version )([0-9\.]+)', dump)[0]
    
    client = docker.from_env()

    try:
        container = client.containers.run(
            f"postgres:{db_version}",
            detach=True,
            environment=[
                f"POSTGRES_USER={username}",
                f"POSTGRES_DB={db_name}",
                f"POSTGRES_PASSWORD={username}"
            ],
            ports={5432: 5432},
        )
        time.sleep(2) # too lazy to write a readiness probe

        logging.info(f"Launched PostgreSQL container {container.id}")
        logging.info(f"Restoring PostgreSQL dump to {container.id}")

        # Db restore
        try:
            restore = subprocess.run(
                [
                    "docker", "exec", "-i", container.id,
                    "psql", "-d", db_name, "-U", username, "-h", "localhost", "-p", "5432"
                ],
                input=dump.encode(),
                stderr=subprocess.STDOUT
            )
            if restore.returncode != 0:
                logging.error(
                    f"Failed to restore PostgreSQL dump to {container.id}"
                    f"\n{restore.stdout}"
                )
                sys.exit(1)
            logging.info(f"Restored PostgreSQL dump to {container.id}")
        except subprocess.CalledProcessError as e:
            logging.error(
                f"Failed to restore PostgreSQL dump to {container.id}"
                f"\n{e}"
            )
            sys.exit(1)

        # dealing with results
        results = get_db_records(
            usr=username,
            pwd=username,
            hst="localhost",
            db=db_name,
            port=5432
        )
        logging.info(results)
        print(submit_solution(access_token, results))

    finally:
        logging.info(f"Stopping PostgreSQL container {container.id}")
        container.stop()
        logging.info(f"Stopped PostgreSQL container {container.id}")
        logging.info(f"Removing PostgreSQL container {container.id}")
        container.remove()
