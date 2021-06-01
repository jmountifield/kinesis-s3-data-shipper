import sys
import datetime
import tempfile
import gzip
import subprocess
import os
import json
import shutil
import urllib.parse
import sqlite3
import fnmatch
import re
from collections import deque
import urllib3
import boto3
import time


def humio_url(args):
    """Return the URL to Humio's structured ingest API"""
    return urllib.parse.urljoin(args["humio-host"], "/api/v1/ingest/humio-structured")


def humio_headers(args):
    """Humio structured api endpoint"""
    return {
        "Content-Type": "application/json",
        "Content-Encoding": "gzip",
        "Authorization": "Bearer " + args["humio-token"],
    }


def log(message, level="INFO"):
    """A cheap little log line printer"""
    print("%s [%s] %s" % (datetime.datetime.now(), level, message))


def is_compressed(filename):
    """Detects if a file at a specific path is gzip compressed."""
    try:
        gzip.GzipFile(filename=filename).peek(64)
        return True
    except OSError:
        return False


def initalise_connection(db_file):
    """create a database connection to the SQLite database
    specified by the db_file"""
    conn = None
    try:
        conn = sqlite3.connect(db_file, timeout=60)
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS files (
                         bucket text NOT NULL,
                         filepath text NOT NULL); """
        )
    except sqlite3.Error as e:
        print(e)
    return conn


def track_file(conn, bucket, filepath):
    """update priority, begin_date, and end date of a task"""
    cur = conn.cursor()
    cur.execute("INSERT INTO files VALUES (?,?)", (bucket, filepath))
    conn.commit()


def parse_and_send(args, file, http, client):
    """For the file given we will extract the relevent events, parse them, and
    send them to Humio."""

    # Using a global variable for event queue
    global events_to_process

    # As we process the files, if tracking is requested, we need to register them as done
    if args["track"]:
        conn = initalise_connection(args["track"])

    # We're going to process each file in a dedicated temp directory
    with tempfile.TemporaryDirectory(dir=args["tmpdir"]) as tmpdirname:

        # Get the filename from the file path
        file_path = os.path.join(tmpdirname, os.path.basename(file))

        # Download the source file from S3
        client.download_file(args["bucket"], file, file_path)

        # Now we may need to decompress the file, and potentially even TWICE, eurgh
        while is_compressed(file_path):
            # Decompress the file in-place
            with gzip.open(file_path, "rb") as f_in, open(
                file_path + ".working", "wb"
            ) as f_out:
                shutil.copyfileobj(f_in, f_out)
            # Move the new file over the top of the original
            shutil.move(file_path + ".working", file_path)

        # Rewite the cloudtrail file to convert the records[] array to NDJSON
        try:
            with open(file_path + ".working", "wb") as f_out:
                subprocess.run(
                    ['jq -c ".Records | .[]" %s' % file_path],
                    shell=True,
                    check=True,
                    stdout=f_out,
                )
        except subprocess.CalledProcessError as e:
            # Probably means we have an empty JQ file
            log("Failed to run jq, probably an empty input file %s" % e, level="ERROR")

            # Skip this iteration of the loop as there's not going to be a vaild file to process
            return False

        # Moving the working file back in place
        shutil.move(file_path + ".working", file_path)

        # TODO: at this point it is almost certianly more effecient to just POST
        # the data directly to Humio as a compressed NDJSON against HEC raw

        # Keep track of how many events we process from this file
        events_added = 0

        # Open the new uncompressed and reformatted file for line-by-line processing
        with open(file_path) as ct_events_fh:

            # Iterate over the lines in the file (should be NDJSON)
            for event in ct_events_fh:
                # Add the event to the list of events to send
                json_event = json.loads(event)

                # Add the details of the source of the event
                json_event["s3_key"] = file
                json_event["s3_bucket"] = args["bucket"]

                events_to_process.append(json_event)
                events_added += 1

                # If we have enough events already, send them now and reset the list
                if len(events_to_process) == args["humio_batch"]:
                    send_events_to_humio(args, events_to_process, http)
                    events_to_process = []

        # We have queued (and maybe sent) all the events from this file
        if args["debug"]:
            log(
                "Queued %d items from file: %s" % (events_added, file),
                level="DEBUG",
            )

        # We have processed a whole file, if tracking is requested we need to register it
        if args["track"]:
            track_file(conn, args["bucket"], file)

    if args["track"]:
        conn.close()


def send_events_to_humio(args, events, http):
    """Takes groups of events and sends them in a single request to Humio. Does not
    validate the number of events, so don't pass too many! (or none).

    events: a list of JSON objects (each object a log event)
    """

    # Set the default tags, and we're going to only be sending one tag combination
    payload = [{"tags": {}, "events": []}]
    # payload[0]["tags"]["provider"] = "aws"
    # payload[0]["tags"]["service"] = "cloudtrail"

    # Process each event to build the payload
    for event in events:
        payload[0]["events"].append(
            {"timestamp": event["eventTime"], "attributes": event}
        )

    encoded_data = json.dumps(payload, sort_keys=True, indent=4).encode("utf-8")
    encoded_compressed = gzip.compress(encoded_data, compresslevel=6)

    r = http.request(
        "POST", humio_url(args), body=encoded_compressed, headers=humio_headers(args)
    )
    if r.status == 200:
        log("Sent %d events" % len(events), level="INFO")
    else:
        log(
            "Failed to send %d events to Humio, status=%d"
            % (len(payload[0]["events"]), r.status),
            level="ERROR",
        )

    return


def find_s3_subfolders(
    client,
    bucket,
    depth_hint=5,
    prefix="",
    CommonPrefixes=deque([]),
    ContinuationToken=False,
):
    """This function can be directed to an S3 bucket and will return the subdirectories found
    therein, as much as S3 has subdirectories."""

    # Call to S3 to get more common prefixes
    if ContinuationToken:
        object_list = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/",
            ContinuationToken=ContinuationToken,
        )
    else:
        object_list = client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter="/"
        )

    # Add the newly found prefixes to our list to process, if there are any
    if "CommonPrefixes" in object_list:
        for CommonPrefix in object_list["CommonPrefixes"]:
            if len(CommonPrefix["Prefix"].split("/")) <= depth_hint:
                CommonPrefixes.append(CommonPrefix)

    # If we didn't get all the prefixes from this run, run again with the same options + ContinuationToken
    if object_list["IsTruncated"]:
        yield from find_s3_subfolders(
            client,
            bucket,
            depth_hint,
            prefix,
            CommonPrefixes,
            object_list["NextContinuationToken"],
        )

    # Throw up the next prefix to process, it's a valid prefix
    try:
        prefix = CommonPrefixes.popleft()["Prefix"]
    except IndexError:
        # The list is empty so we're done
        return

    # This is where we actually return a prefix we've found
    yield prefix

    # If we've still got common prefixes then we need to run again on the next prefixes
    yield from find_s3_subfolders(client, bucket, depth_hint, prefix, CommonPrefixes)


def find_shortest_common_prefixes(client, bucket, prefix=""):
    """Given a particular prefix pattern in S3, find the shortest directories
    that match that pattern."""

    # Shrten the prefix until we get to the last forward slash (everything
    # after could be a filename match)
    while not prefix.endswith("/") and len(prefix) > 0:
        prefix = prefix[:-1]

    # If no prefix is sent return a list with the empty string
    if prefix == "":
        return [""]
    else:
        literal_prefix = re.match("^([^\*\?]+)", prefix).group()

    # If the prefix is unchanged by the pattern then return it straight away
    if prefix == literal_prefix:
        return [prefix]

    common_prefixes = []
    shortest_segments = 999999999

    depth_hint = len(prefix.split("/")) + 1

    for subfolder in find_s3_subfolders(client, bucket, depth_hint, literal_prefix):
        # Does the subfolder match?
        if fnmatch.fnmatch(subfolder, prefix):
            # Check to make sure we're not adding a deeper subfolder
            segment_length = len(subfolder.split("/"))
            # If the new match is longer than the shortest ones we have already
            # then we can return the list without adding it
            if segment_length > shortest_segments:
                return common_prefixes
            else:
                shortest_segments = segment_length
            # Add the subfolder to the list, it matches and is the same length
            # as any already matching subfolders
            common_prefixes.append(subfolder)

    # If we get here it means we probably found only one depth of subfolders
    return common_prefixes


def find_files(args, client, common_prefix):
    """ Get a list of all the objects to process"""

    # Lets see if we have a prefix with wildcards. We can't use them when we get the file list from
    # AWS but we can post-process the file list to make sure they match
    if args["prefix"]:
        prefix = re.match("^([^\*\?]+)", args["prefix"]).group()
    else:
        prefix = ""

    objects = []
    continuation_token = "UNSET"
    while continuation_token:
        if continuation_token == "UNSET":
            object_list = client.list_objects_v2(
                Bucket=args["bucket"], Prefix=common_prefix
            )
        else:
            object_list = client.list_objects_v2(
                Bucket=args["bucket"],
                Prefix=common_prefix,
                ContinuationToken=continuation_token,
            )

        if args["debug"]:
            log(
                "Found %d items from bucket list_objects_v2(), includes dirs."
                % object_list["KeyCount"],
                level="DEBUG",
            )

        # This means we have no more keys, or none found
        if object_list["KeyCount"] > 0:
            for item in object_list["Contents"]:
                if not item["Key"].endswith("/") and fnmatch.fnmatch(
                    item["Key"], args["prefix"] + "*"
                ):  # ignore directories
                    objects.append(item["Key"])

        # And here we check to see if there's more results to recover
        if object_list["IsTruncated"]:
            continuation_token = object_list["NextContinuationToken"]
        else:
            continuation_token = False

    # What did we get?
    log("Found %d items" % len(objects))

    # If we have a tracking database argument we need to dedupe the list against already
    # processed files
    if args["track"]:
        conn = initalise_connection(args["track"])
        for filepath in conn.execute(
            """SELECT filepath FROM files WHERE bucket=? AND filepath LIKE ?""",
            (args["bucket"], prefix + "%"),
        ):
            if filepath[0] in objects:
                objects.remove(filepath[0])
                if args["debug"]:
                    log(
                        "Excluding already processed file %s" % filepath[0],
                        level="DEBUG",
                    )
        conn.close()

    return objects


def is_suitable_tempdir(path):
    if os.path.isdir(path) and os.access(path, os.W_OK):
        return path
    msg = "%s is not a usable temp dir" % path
    raise argparse.ArgumentTypeError(msg)


def not_implemented(token):
    msg = "This argument is not currently supported."
    raise argparse.ArgumentTypeError(msg)


def pp_args(args):
    print("Running with the following arguments:")
    print()
    for arg in args:
        argNamePadded = "{:<18}".format(arg)
        if arg in ["aws_access_secret", "humio-token"]:
            print("\t%s =>\t%s" % (argNamePadded, str("*" * len(str(args[arg])))))
        else:
            print("\t%s =>\t%s" % (argNamePadded, str(args[arg])))
    print()


def get_new_events(args, sqs, maxEvents=1, maxWaitSeconds=10, reserveSeconds=300):
    queue = sqs.Queue(args["sqs_queue"])
    return queue.receive_messages(
        MessageAttributeNames=["All"],
        WaitTimeSeconds=maxWaitSeconds,
        VisibilityTimeout=reserveSeconds,
        MaxNumberOfMessages=maxEvents,
    )


# def sqs_files(args, sqs, common_prefix):
#     objects = []


#     for key in objects:
#         if not key.startswith(common_prefix):


if __name__ == "__main__":

    # We only need to do the argparse if we're running interactivley
    import argparse

    parser = argparse.ArgumentParser(
        description="This script is used to coordinate log ingest from S3 where those logs have arrived via an AWS kinesis stream."
    )

    # Details for the source bucket and access
    parser.add_argument(
        "bucket",
        type=str,
        action="store",
        help='The S3 bucket from which to export. E.g "demo.humio.xyz"',
    )
    parser.add_argument(
        "--sqs-queue",
        type=str,
        action="store",
        help="SQS queue URL. When specified will read events from SQS queue to look for new changes.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        action="store",
        default="",
        help='The S3 prefix from which to search. E.g "awslogs/2020/04/01/"',
    )
    parser.add_argument(
        "--aws-access-id",
        type=not_implemented,
        action="store",
        help="The AWS access key ID (not implemented)",
    )
    parser.add_argument(
        "--aws-access-secret",
        type=not_implemented,
        action="store",
        help="The AWS access key secret (not implemented)",
    )

    # Target system where the logs will be sent
    parser.add_argument(
        "humio-host",
        type=str,
        action="store",
        default="https://cloud.humio.com/",
        help="The URL to the target Humio instance, including port number",
    )
    parser.add_argument(
        "humio-token", type=str, action="store", help="Ingest token for this input"
    )
    parser.add_argument(
        "--humio-batch",
        type=int,
        action="store",
        default=5000,
        help="max event batch size for Humio API",
    )
    parser.add_argument(
        "--sleep-time",
        type=int,
        action="store",
        default=5,
        help="Number of seconds to sleep when running continuous",
    )

    # Are we going to do the debug?
    parser.add_argument("--debug", action="store_true", help="We do the debug?")
    parser.add_argument("--continuous", action="store_true", help="Run continuously?")
    parser.add_argument(
        "--tmpdir",
        type=is_suitable_tempdir,
        action="store",
        default="/tmp",
        help="The temp directory where the work will be done",
    )
    parser.add_argument(
        "--track",
        type=str,
        action="store",
        help="A path for a sqlite database for tracking files successfully processed",
    )

    # Build the argument list
    args = vars(parser.parse_args())

    # Echo to stdout so we can see the args in use if debug
    if args["debug"]:
        pp_args(args)

    # Initialise the S3 client
    client = boto3.client("s3")
    sqs_client = boto3.client("sqs")
    sqs = boto3.resource("sqs")

    # We're going to start sending some events so setup the http connection pool
    http = urllib3.PoolManager()

    # Maintain a global state of events buffered for sending to Humio
    global events_to_process
    events_to_process = []

    sleep_backoff = args["sleep_time"]

    while True:

        did_work = False

        if args["sqs_queue"]:
            for message in get_new_events(args, sqs, maxEvents=10):
                newObjectEvent = json.loads(message.body)
                try:
                    for record in newObjectEvent["Records"]:
                        file = record["s3"]["object"]["key"]
                        if file.startswith(args["prefix"]):
                            parse_and_send(args, file, http, client)
                        else:
                            if args["debug"]:
                                log(
                                    "Skipping key %s as does not match prefix"
                                    % args["prefix"],
                                    level="DEBUG",
                                )
                except KeyError as e:
                    log(
                        "Skipping invalid SQS message:\n\n%s" % newObjectEvent,
                        level="WARN",
                    )
                # Remove the item from the queue now it's processed
                message.delete()

                did_work = True

        else:
            # Find the files to download and process as we're doing a scan
            for common_prefix in find_shortest_common_prefixes(
                client, args["bucket"], args["prefix"]
            ):

                log("Collecting files for prefix: %s" % common_prefix)

                filesToProcess = find_files(args, client, common_prefix)

                if not filesToProcess:
                    log(
                        "No Objects Found. If you think there should be then check yo' prefix."
                    )
                else:
                    # Process each file that we want to send to Humio
                    for file in sorted(filesToProcess):
                        parse_and_send(args, file, http, client)

                    did_work = True

        # Flush the events before sleep
        if events_to_process:
            send_events_to_humio(args, events_to_process, http)
            events_to_process = []

        if not args["continuous"]:
            break
        else:
            # We're running in continuous mode
            # if we did no work this time around double the sleep up to 5 mins
            # if we did do work then reset sleep timer
            if did_work:
                sleep_backoff = args["sleep_time"]
            else:
                sleep_backoff = min(max(sleep_backoff, 1) * 2, 300)

            log("Sleeping for %d seconds" % sleep_backoff)
            time.sleep(sleep_backoff)

    sys.exit()
