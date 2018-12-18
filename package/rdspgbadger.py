#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Fetch logs from RDS postgres instance and use them with pgbadger to generate a
report.
"""
from __future__ import print_function
import os
import glob
import argparse
import time
import math
import fnmatch
import errno
import boto3
import json
import typing
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoRegionError,
    NoCredentialsError,
    PartialCredentialsError
)

from datetime import datetime, timedelta
try:
    from shutil import which
except ImportError:
    from which import which

import subprocess

import logging

__version__ = "1.2.2@laroo"


pgBadgerLastParsed = typing.NamedTuple('pgBadgerLastParsed', [
    ('datetime', datetime),
    ('current_pos', int),
    ('orig', str),
])


def valid_date(s):
    if s.lower() == 'today':
        return datetime.today().strftime("%Y-%m-%d")
    elif s.lower() == 'yesterday':
        date_delta = datetime.today() - timedelta(days=1)
        return date_delta.strftime("%Y-%m-%d")

    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


parser = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('instance', help="RDS instance identifier (wildcards allowed)")
parser.add_argument('--version', action='version',
                    version='%(prog)s {version}'.format(version=__version__))

parser.add_argument('-v', '--verbose', help="increase output verbosity",
                    action='store_true')
parser.add_argument('-d', '--date', help="get logs for given YYYY-MM-DD date (or use relative: 'today' or 'yesterday')",
                    type=valid_date)
parser.add_argument('-D', '--delete', help="Delete logs already parsed by pgbadger (using pg_badger's LAST_PARSED)",
                    action='store_true')
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


def get_rds_client(region=None, assume_role=None):
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

    rds_client = boto3.client("rds", **boto_args)
    return rds_client


def get_db_instances(rds_client, db_instance_pattern):

    db_instances = []
    for described_db_instance in rds_client.describe_db_instances()['DBInstances']:
        instance_id = described_db_instance['DBInstanceIdentifier']
        logger.debug("Checking pattern '{}' against DB instance '{}'".format(db_instance_pattern, instance_id))
        if fnmatch.fnmatch(instance_id, db_instance_pattern):
            db_instances.append(instance_id)

    logger.debug("Included DB instances: {}".format(', '.join(db_instances)))
    return db_instances


def get_all_logs(rds_client, db_instance_id, output_dir, date=None):

    # Test ClientErrors:
    #     "ERROR :: An error occurred (403) when calling the PutObject operation: Unauthorized"
    #     # raise ClientError({'Error': {'Code': '403', 'Message': 'Unauthorized'}}, 'PutObject')
    #
    #     "ERROR :: An error occurred (Throttling) when calling the DownloadDBLogFilePortion operation: Rate exceeded"
    #     "ERROR :: An error occurred (Throttling) when calling the DownloadDBLogFilePortion operation (reached max retries: 4): Rate exceeded"
    #     raise ClientError({'Error': {'Code': 'Throttling', 'Message': 'Rate exceeded'}}, 'DownloadDBLogFilePortion')

    paginator = rds_client.get_paginator("describe_db_log_files")
    response_iterator = paginator.paginate(
        DBInstanceIdentifier=db_instance_id,
        FilenameContains="postgresql.log"
    )

    total_log_file_size = 0
    for response in response_iterator:
        for log in (name for name in response.get("DescribeDBLogFiles")
                    if not date or date in name["LogFileName"]):

            log_file_name = log['LogFileName']
            log_file_timestamp = log['LastWritten']
            log_file_size = log['Size']
            total_log_file_size += log_file_size

            filename = "{}/{}/{}".format(output_dir, db_instance_id, log["LogFileName"])
            logger.info("Downloading file {} ({} - {})".format(
                filename, format_timestamp(log_file_timestamp), convert_size(log_file_size)))

            if log_file_exists(filename) and log_file_size_match(filename, log_file_size):
                logger.debug('skipping, log with same size already exists')
                continue

            if not os.path.exists(os.path.dirname(filename)):
                try:
                    os.makedirs(os.path.dirname(filename))
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            write_log(rds_client, db_instance_id, filename, log_file_name)

            # TODO Should be a CLI parameter to enable these stats
            local_log_file_stat = os.stat(filename)
            stats = {
                'log_file_name': log_file_name,

                'remote_size_bytes': log_file_size,
                'remote_size_formatted': convert_size(log_file_size),
                'remote_last_written_timestamp': log_file_timestamp,
                'remote_last_written_formatted': format_timestamp(log_file_timestamp),

                'local_size_bytes': local_log_file_stat.st_size,
                'local_size_formatted': convert_size(local_log_file_stat.st_size),
                'local_last_written_timestamp': local_log_file_stat.st_atime,
                'local_last_written_formatted': format_timestamp(local_log_file_stat.st_atime),

                'diff_size_bytes': log_file_size - local_log_file_stat.st_size,
            }
            stats_file = '{}.stats.json'.format(filename)
            with open(stats_file, "w") as log_info:
                log_info.write(json.dumps(stats, indent=4, sort_keys=True))
                log_info.write("\n")

    logger.debug('Total log file size: {0: <12}'.format(convert_size(total_log_file_size)))


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

    logger.debug('Log file size difference: file={} - api={} = {} diff, '.format(file_size, size, diff_pct))

    if diff_pct > 0.1:
        return False

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
    if '.' in str(timestamp):
        # Float timestamp
        dt = datetime.fromtimestamp(float(timestamp))
    else:
        # Integer, so divide by 1000
        dt = datetime.fromtimestamp(int(timestamp)/1000.0)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def download_partial_log_file(client, db_instance_id, logfilename, marker, max_number_of_lines):
    try:
        response = client.download_db_log_file_portion(
            DBInstanceIdentifier=db_instance_id,
            LogFileName=logfilename,
            Marker=marker,
            NumberOfLines=max_number_of_lines
        )
    except ClientError as e:
        if e.response.get('Error').get('Code') == 'Throttling':
            logger.debug('Received throttling error... retrying after 10 seconds')
            time.sleep(10)
            return download_partial_log_file(client, db_instance_id, logfilename, marker, max_number_of_lines)
        raise e

    return response


def write_log(client, db_instance_id, filename, logfilename):
    marker = "0"
    max_number_of_lines = 10000
    subtract_lines = 10
    truncated_string = " [Your log message was truncated]"
    slice_length = len(truncated_string) + 1

    try:
        os.remove(filename)
    except OSError:
        pass

    with open(filename, "a") as logfile:
        while True:

            response = download_partial_log_file(client, db_instance_id, logfilename, marker, max_number_of_lines)

            if 'Marker' in response and response['Marker'] == marker:
                logger.debug('Marker is the same... breaking...')
                break

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
                    # max_number_of_lines = 10000  # No truncated, so reset
                    marker = response["Marker"]
                    logger.debug("Writing marker '{}' ({} lines) to {}".format(marker, max_number_of_lines, logfilename))
                    logfile.write(response["LogFileData"])

            if ('LogFileData' in response and
                    not response["LogFileData"].rstrip("\n") and
                    not response["AdditionalDataPending"]):
                break


def delete_parsed_logs(db_instance_id, output_dir, date=None):
    logger.info('Checking logs for deletion: {}'.format(db_instance_id))

    last_parsed_file_path = '{}/LAST_PARSED'.format(get_instance_log_dir(db_instance_id=db_instance_id, output_dir=output_dir))
    last_parsed = read_pgbadger_last_parsed(last_parsed_file_path)
    if not last_parsed:
        # No or corrupt LAST_PARSED found, skip...
        return False

    parsed_date_pattern = last_parsed.datetime.strftime("%Y-%m-%d")
    log_file_pattern = get_instance_log_file_pattern(db_instance_id=db_instance_id, output_dir=output_dir, date=date)

    # Get log files sorted by name, compare timestamp and skip rest if a match is found
    skip_following = False
    for log_file in sorted(glob.glob(log_file_pattern)):
        if skip_following or parsed_date_pattern in log_file:
            logger.debug("Skipping {} ".format(log_file))
            skip_following = True
        else:
            try:
                logger.info("Deleting parsed log file {} ".format(log_file))
                # os.remove(log_file)
            except OSError:
                logger.error("Could not delete parsed log file {}".format(log_file))


def read_pgbadger_last_parsed(last_parsed_file_path):
    """
    Reads the LAST_PARSED file and returns a namedtuple with the info

    From pgbadger:
        my ($datetime, $current_pos, $orig, @others) = split(/\t/, $line);
        # Last parsed line with pgbouncer log starts with this keyword
        if ($datetime eq 'pgbouncer') {
            $pgb_saved_last_line{datetime} = $current_pos;
            $pgb_saved_last_line{current_pos} = $orig;
            $pgb_saved_last_line{orig} = join("\t", @others);
        } else {
            $saved_last_line{datetime} = $datetime;
            $saved_last_line{current_pos} = $current_pos;
            $saved_last_line{orig} = $orig;
        }
    """
    if not os.path.isfile(last_parsed_file_path):
        logger.debug("No LAST_PARSED file found at '{}'".format(last_parsed_file_path))
        return False

    with open(last_parsed_file_path) as f:
        log_line = f.readline()

    fields = log_line.split("\t")
    if len(fields) < 3:
        logger.debug("Expected LAST_PARSED file to have at least 3 fields, found {}".format(len(fields)))
        return False
    elif fields[0] == 'pgbouncer':
        raise NotImplementedError('pgbouncer is not supported!')

    return pgBadgerLastParsed(
        datetime=datetime.strptime(fields[0], '%Y-%m-%d %H:%M:%S'),
        current_pos=int(fields[1]),
        orig=fields[2],
    )


def get_instance_log_file_pattern(db_instance_id, output_dir, date=None):
    log_dir = get_instance_log_dir(db_instance_id, output_dir)
    if date:
        return "{}/error/postgresql.log.{}*".format(log_dir, date)
    return "{}/error/postgresql.log.*".format(log_dir)


def get_instance_log_dir(db_instance_id, output_dir):
    return "{}/{}".format(output_dir, db_instance_id)


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
        rds_client = get_rds_client(region=args.region, assume_role=args.assume_role)
        db_instances = get_db_instances(rds_client, args.instance)

        for db_instance_id in db_instances:
            logger.info('Downloading logs: {}'.format(db_instance_id))
            get_all_logs(
                rds_client=rds_client,
                db_instance_id=db_instance_id,
                output_dir=args.output,
                date=args.date
            )
    except (EndpointConnectionError, ClientError) as e:
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
        for db_instance_id in db_instances:

            logger.info('Generating PG Badger report: {}'.format(db_instance_id))

            command = ("{} -p \"%t:%r:%u@%d:[%p]:\" {} -o {}/report.{} {} ".format(
                pgbadger,
                args.pgbadger_args,
                get_instance_log_dir(db_instance_id=db_instance_id, output_dir=args.output),
                args.format,
                get_instance_log_file_pattern(db_instance_id=db_instance_id, output_dir=args.output, date=args.date)
            ))
            logger.debug("Command: %s", command)
            subprocess.call(command, shell=True)

    if args.delete:
        logger.info("Deleting logs that are already parsed by pgbadger")
        for db_instance_id in db_instances:
            delete_parsed_logs(
                db_instance_id=db_instance_id,
                output_dir=args.output,
                date=args.date
            )

    logger.info("Done")


if __name__ == '__main__':
    main()
