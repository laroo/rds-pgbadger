#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Fetch logs from RDS postgres instance and use them with pgbadger to generate a
report.
"""

import os
import sys
import time
import math
import errno
import boto3
from botocore.exceptions import (ClientError,
                                 EndpointConnectionError,
                                 NoRegionError,
                                 NoCredentialsError,
                                 PartialCredentialsError)
import argparse
from datetime import datetime
try:
    from shutil import which
except ImportError:
    from which import which

import subprocess

import logging

__version__ = "1.2.2"


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


parser = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('instance', help="RDS instance identifier")
parser.add_argument('--version', action='version',
                    version='%(prog)s {version}'.format(version=__version__))

parser.add_argument('-v', '--verbose', help="increase output verbosity",
                    action='store_true')
parser.add_argument('-d', '--date', help="get logs for given YYYY-MM-DD date",
                    type=valid_date)
parser.add_argument('--assume-role', help="AWS STS AssumeRole")
parser.add_argument('-r', '--region', help="AWS region")
parser.add_argument('-o', '--output', help="Output folder for logs and report",
                    default='out')
parser.add_argument('-n', '--no-process', help="Only download logs",
                    action='store_true')
parser.add_argument('-X', '--pgbadger-args', help="pgbadger arguments",
                    default='')
parser.add_argument('-f', '--format', help="Format of the report",
                    choices=['text', 'html', 'bin', 'json', 'tsung'],
                    default='html')

logger = logging.getLogger("rds-pgbadger")


def define_logger(verbose=False):
    logger = logging.getLogger("rds-pgbadger")
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logFormatter = logging.Formatter("%(asctime)s :: %(levelname)s :: "
                                     "%(message)s")
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)


def get_all_logs(dbinstance_id, output,
                 date=None, region=None, assume_role=None):

    # try:
    #     "ERROR :: An error occurred (403) when calling the PutObject operation: Unauthorized"
    #     # raise ClientError({'Error': {'Code': '403', 'Message': 'Unauthorized'}}, 'PutObject')
    #
    #     "ERROR :: An error occurred (Throttling) when calling the DownloadDBLogFilePortion operation: Rate exceeded"
    #
    #     "ERROR :: An error occurred (Throttling) when calling the DownloadDBLogFilePortion operation (reached max retries: 4): Rate exceeded"
    #     raise ClientError({'Error': {'Code': 'Throttling', 'Message': 'Rate exceeded'}}, 'DownloadDBLogFilePortion')
    # except ClientError as e:
    #     print(type(e))
    #     print(e.__dict__)
    #     print(e.response.get('Error').get('Code'))
    #     raise e

    boto_args = {}
    if region:
        boto_args['region_name'] = region

    if assume_role:
        sts_client = boto3.client('sts')
        assumedRoleObject = sts_client.assume_role(
                RoleArn=assume_role,
                RoleSessionName="RDSPGBadgerSession1"
        )

        credentials = assumedRoleObject['Credentials']
        boto_args['aws_access_key_id'] = credentials['AccessKeyId']
        boto_args['aws_secret_access_key'] = credentials['SecretAccessKey']
        boto_args['aws_session_token'] = credentials['SessionToken']
        logger.info('STS Assumed role %s', assume_role)

    client = boto3.client("rds", **boto_args)
    paginator = client.get_paginator("describe_db_log_files")
    response_iterator = paginator.paginate(
        DBInstanceIdentifier=dbinstance_id,
        FilenameContains="postgresql.log"
    )

    for response in response_iterator:
        for log in (name for name in response.get("DescribeDBLogFiles")
                    if not date or date in name["LogFileName"]):

            filename = "{}/{}".format(output, log["LogFileName"])
            logger.info("Downloading file %s", filename)

            log_file_name = log['LogFileName']
            log_file_timestamp = log['LastWritten']
            log_file_size = log['Size']
            print('{0: <40} {1: <26} {2: <12}   '.format(log_file_name, format_timestamp(log_file_timestamp),
                                                         convert_size(log_file_size)))

            if log_file_exists(filename) and log_file_size_match(filename, log_file_size):
                print('skipping')
                continue

            try:
                os.remove(filename)
            except OSError:
                pass
            write_log(client, dbinstance_id, filename, log["LogFileName"])


def log_file_local_path(log_file_name):
    return log_file_name


def log_file_exists(log_file_name):
    if os.path.isfile(log_file_local_path(log_file_name)):
        return True
    return False


def log_file_size_match(log_file_name, size):
    full_path = log_file_local_path(log_file_name)
    if not os.path.isfile(full_path):
        return False

    file_size = os.path.getsize(full_path)
    diff = int(file_size) - int(size)

    if diff == 0:
        return True

    diff_pct = abs((100.0/size) * file_size)

    print('size diff: file={} - api={} = {} diff, '.format(file_size, size, diff_pct), end='')

    if diff_pct > 0.1:
        print(' NOMATCH ')
        return False

    print(' MATCH ')
    return True


def convert_size(size):
    if size == 0:
        return '0B'
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size/p, 2)
    return '%s %s' % (s, size_name[i])


def format_timestamp(timestamp):
    dt = datetime.fromtimestamp(int(timestamp)/1000.0)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def download_partial_log_file(client, dbinstance_id, logfilename, marker, max_number_of_lines):
    try:
        response = client.download_db_log_file_portion(
            DBInstanceIdentifier=dbinstance_id,
            LogFileName=logfilename,
            Marker=marker,
            NumberOfLines=max_number_of_lines
        )
    except ClientError as e:
        if e.response.get('Error').get('Code') == 'Throttling':
            print('Received throttling error... retrying after 10 seconds')
            time.sleep(10)
            return download_partial_log_file(client, dbinstance_id, logfilename, marker, max_number_of_lines)
        raise e

    return response


def write_log(client, dbinstance_id, filename, logfilename):
    marker = "0"
    max_number_of_lines = 10000
    subtract_lines = 10
    truncated_string = " [Your log message was truncated]"
    slice_length = len(truncated_string) + 1

    data_pending = True
    cnt = 0

    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(filename, "a") as logfile:
        while data_pending:

            # response = client.download_db_log_file_portion(
            #     DBInstanceIdentifier=dbinstance_id,
            #     LogFileName=logfilename,
            #     Marker=marker,
            #     NumberOfLines=max_number_of_lines
            # )
            response = download_partial_log_file(client, dbinstance_id, logfilename, marker, max_number_of_lines)

            if response['Marker'] == marker:
                print('Marker is the same... breaking...')
                break
            cnt += 1

            if 'LogFileData' in response:
                if truncated_string in response["LogFileData"][-slice_length:]:
                    downloaded_lines = response["LogFileData"].count("\n")
                    if downloaded_lines == 0:
                        raise Exception(
                            "No line was downloaded in last portion!")
                    max_number_of_lines = max(
                        downloaded_lines - subtract_lines, 1)
                    logger.info("Log truncated, retrying portion with "
                                "NumberOfLines = {0}".format(
                                    max_number_of_lines))
                else:
                    marker = response["Marker"]
                    logfile.write(response["LogFileData"])

                    # print('.', end='')
                    # sys.stdout.flush()

            if ('LogFileData' in response and
                    not response["LogFileData"].rstrip("\n") and
                    not response["AdditionalDataPending"]):
                break
            #
            # response = client.download_db_log_file_portion(
            #     DBInstanceIdentifier=dbinstance_id,
            #     LogFileName=logfilename,
            #     Marker=marker,
            #     NumberOfLines=max_number_of_lines
            # )


def main():
    args = parser.parse_args()
    define_logger(args.verbose)

    if args.date:
        logger.info("Getting logs from %s", args.date)
    else:
        logger.info("Getting all logs")

    pgbadger = which("pgbadger")
    if not pgbadger:
        raise Exception("pgbadger not found")
    logger.debug("pgbadger found")

    try:
        get_all_logs(
                args.instance,
                args.output,
                date=args.date,
                region=args.region,
                assume_role=args.assume_role
            )
    except (EndpointConnectionError, ClientError) as e:
        print('-'*100)
        print(type(e))
        print('-' * 100)
        print(e.__dict__)
        print('-' * 100)
        logger.error(e)
        exit(1)
    except NoRegionError:
        logger.error("No region provided")
        exit(1)
    except NoCredentialsError:
        logger.error("Missing credentials")
        exit(1)
    except PartialCredentialsError:
        logger.error("Partial credentials, please check your credentials file")
        exit(1)

    if args.no_process:
        logger.info("File(s) downloaded. Not processing with PG Badger.")
    else:
        logger.info("Generating PG Badger report.")
        command = ("{} -p \"%t:%r:%u@%d:[%p]:\" {} -o {}/report.{} "
                   "{}/error/*.log.* ".format(pgbadger,
                                              args.pgbadger_args,
                                              args.output,
                                              args.format,
                                              args.output))
        logger.debug("Command: %s", command)
        subprocess.call(command, shell=True)
        logger.info("Done")


if __name__ == '__main__':
    main()
