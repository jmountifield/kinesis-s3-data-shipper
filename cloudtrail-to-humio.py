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
import urllib3
import boto3


def humio_url(args):
    """Return the URL to Humio's structured ingest API"""
    return urllib.parse.urljoin(args['humio-host'], '/api/v1/ingest/humio-structured')



def humio_headers(args):
    """Humio structured api endpoint"""
    return {'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + args['humio-token']}



def log(message, level='INFO'):
    """A cheap little log line printer"""
    print("%s [%s] %s"% (datetime.datetime.now(), level, message))



def is_compressed(filename):
    """Detects if a file at a specific path is gzip compressed."""
    try:
        gzip.GzipFile(filename=filename).peek(64)
        return True
    except OSError:
        return False



def initalise_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS files (
                         bucket text NOT NULL,
                         filepath text NOT NULL); """)
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
    if args['track']:
        conn = initalise_connection(args['track'])


    # We're going to process each file in a dedicated temp directory
    with tempfile.TemporaryDirectory(dir=args['tmpdir']) as tmpdirname:

        # Get the filename from the file path
        file_path = os.path.join(tmpdirname, os.path.basename(file))

        # Download the source file from S3
        client.download_file(args['bucket'], file, file_path)

        # Now we may need to decompress the file, and potentially even TWICE, eurgh
        while is_compressed(file_path):
            # Decompress the file in-place
            with gzip.open(file_path, 'rb') as f_in, open(file_path + ".working", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            # Move the new file over the top of the original
            shutil.move(file_path + ".working", file_path)

        # Rewite the cloudtrail file to convert the records[] array to NDJSON
        with open(file_path + ".working", 'wb') as f_out:
            subprocess.run(["jq -c \".Records | .[]\" %s"% file_path],
                           shell=True, check=True, stdout=f_out)
        # Moving the working file back in place
        shutil.move(file_path + ".working", file_path)

        # Keep track of how many events we process from this file
        events_added = 0

        # Open the new uncompressed and reformatted file for line-by-line processing
        with open(file_path) as ct_events_fh:


            # Iterate over the lines in the file (should be NDJSON)
            for event in ct_events_fh:
                # Add the event to the list of events to send
                events_to_process.append(json.loads(event))
                events_added += 1

                # If we have enough events already, send them now and reset the list
                if len(events_to_process) == args['humio_batch']:
                    send_events_to_humio(args, events_to_process, http)
                    events_to_process = []

        # We have queued (and maybe sent) all the events from this file
        if args['debug']: log("Queued (and maybe already sent) %d items from file: %s"% (events_added, file), level="DEBUG")


        # We have processed a whole file, if tracking is requested we need to register it
        if args['track']:
            track_file(conn, args['bucket'], file)

    if args['track']:
        conn.close()


def send_events_to_humio(args, events, http):
    """Takes groups of events and sends them in a single request to Humio. Does not
    validate the number of events, so don't pass too many! (or none).

    events: a list of JSON objects (each object a log event)
    """

    # Set the default tags, and we're going to only be sending one tag combination
    payload = [ { "tags": {}, "events": [] } ]
    payload[0]['tags']['provider'] =  "aws"
    payload[0]['tags']['service'] = "cloudtrail"

    # Process each event to build the payload
    for event in events:
        payload[0]['events'].append({ "timestamp": event['eventTime'],
                                      "attributes": event })

    encoded_data = json.dumps(payload).encode('utf-8')
    r = http.request('POST', humio_url(args), body=encoded_data, headers=humio_headers(args))
    if r.status == 200:
        log("Sent %d events"% len(events), level="INFO")
    else:
        log("Failed to send %d events to Humio, status=%d"% (len(payload[0]['events']), r.status), level="ERROR")

    return



def find_files(args, client):
    """ Get a list of all the objects to process"""

    # Lets see if we have a prefix with wildcards. We can't use them when we get the file list from
    # AWS but we can post-process the file list to make sure they match
    if args['prefix']:
        prefix = re.match("^([^\*\?]+)", args['prefix']).group()
    else:
        prefix = ""

    objects = []
    continuation_token = 'UNSET'
    while continuation_token:
        if continuation_token == 'UNSET':
            object_list = client.list_objects_v2(Bucket=args['bucket'],Prefix=prefix)
        else:
            object_list = client.list_objects_v2(Bucket=args['bucket'], Prefix=prefix, ContinuationToken=continuation_token)

        if args['debug']: log("Found %d items from bucket list_objects_v2(), includes dirs."% object_list['KeyCount'], level="DEBUG")

        # This means we have no more keys, or none found
        if object_list['KeyCount'] > 0:
            for item in object_list['Contents']:
                if not item['Key'].endswith('/') and fnmatch.fnmatch(item['Key'], args['prefix'] + '*'): # ignore directories
                    objects.append(item['Key'])

        # And here we check to see if there's more results to recover
        if object_list['IsTruncated']:
            continuation_token = object_list['NextContinuationToken']
        else:
            continuation_token = False

    # What did we get?
    log("Found %d items"% len(objects))

    # If we have a tracking database argument we need to dedupe the list against already
    # processed files
    if args['track']:
        conn = initalise_connection(args['track'])
        for filepath in conn.execute('''SELECT filepath FROM files WHERE bucket=? AND filepath LIKE ?''', (args['bucket'], prefix + '%') ):
            if filepath[0] in objects:
                objects.remove(filepath[0])
                if args['debug']: log("Excluding already processed file %s"% filepath[0], level="DEBUG")
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
        if arg in ['aws_access_secret', 'humio-token']:
            print("\t%s =>\t%s"% (argNamePadded, str("*" * len(str(args[arg])))))
        else:
            print("\t%s =>\t%s"% (argNamePadded, str(args[arg])))
    print()



if __name__ == "__main__":

    # We only need to do the argparse if we're running interactivley
    import argparse

    parser = argparse.ArgumentParser(description='This script is used to coordinate log ingest from S3 where those logs have arrived via an AWS kinesis stream.')

    # Details for the source bucket and access
    parser.add_argument('bucket',              type=str, action='store', help='The S3 bucket from which to export. E.g "demo.humio.xyz"')
    parser.add_argument('--prefix',            type=str, action='store', default="", help='The S3 prefix from which to search. E.g "awslogs/2020/04/01/"')
    parser.add_argument('--aws-access-id',     type=not_implemented, action='store', help='The AWS access key ID (not implemented)')
    parser.add_argument('--aws-access-secret', type=not_implemented, action='store', help='The AWS access key secret (not implemented)')

    # Target system where the logs will be sent
    parser.add_argument('humio-host',        type=str, action='store', default="https://cloud.humio.com/", help='The URL to the target Humio instance, including port number')
    parser.add_argument('humio-token',       type=str, action='store', help='Ingest token for this input')
    parser.add_argument('--humio-batch',     type=int, action='store', default=5000, help='max event batch size for Humio API')

    # Are we going to do the debug?
    parser.add_argument('--debug',           action='store_true', help='We do the debug?')
    parser.add_argument('--tmpdir',          type=is_suitable_tempdir, action='store', default="/tmp", help='The temp directory where the work will be done')
    parser.add_argument('--track',           type=str, action='store', help='A path for a sqlite database for tracking files successfully processed')

    # Build the argument list
    args = vars(parser.parse_args())

    # Echo to stdout so we can see the args in use if debug
    if args['debug']: pp_args(args)

    # Initialise the S3 client
    client = boto3.client('s3')

    # Find the files to download and process
    filesToProcess = find_files(args, client)

    if not filesToProcess:
        log("No Objects Found. If you think there should be then check yo' prefix.")
        sys.exit()

    # We're going to start sending some events so setup the http connection pool
    http = urllib3.PoolManager()

    # Maintain a global state of events buffered for sending to Humio
    global events_to_process
    events_to_process = []

    # Process each file that we want to send to Humio
    for file in sorted(filesToProcess):
        parse_and_send(args, file, http, client)

    # Finally send any last events not already processed
    if events_to_process:
        send_events_to_humio(args, events_to_process, http)


    sys.exit()
