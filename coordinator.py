import boto3
import argparse
import sys
import datetime
import tempfile
import gzip
import mmap
import os
import json
import urllib3
import urllib.parse

# This utility is designed to be run against a prefix of an S3 bucket, where
# that prefix typically represents a single day of log data.

parser = argparse.ArgumentParser(description='This script is used to coordinate log ingest from S3.')
parser.add_argument('--bucket', type=str, nargs=1,
                    help='The S3 bucket from which to export. E.g "demo.humio.xyz"')
parser.add_argument('--prefix', type=str, nargs=1,
                    help='The S3 prefix from which to search. E.g "awslogs/2020/04/01/"')
parser.add_argument('--debug', type=bool, nargs='?',
                    help='We do the debug?')
args = parser.parse_args()

# We do the debug?
DEBUG = True if args.debug else False

# These are the credentials for talking to AWS
aws_creds = {'aws_access_key_id':     'AKIAQTPW3VAT6V3GJXX7',
             'aws_secret_access_key': ''}

# This is the target Humio instance and ingest token
humio_creds = {'humio_host':  'http://localhost:8080/',
               'humio_token': 'jOvIdoPPCaTt7ktXzvfLDGB7M8hhWWuNG4jvbP3iPs8t'}

# How many events to send to Humio at a time
humio_api_batch = 1000

# Humio structured api endpoint
humio_url = urllib.parse.urljoin(humio_creds['humio_host'], '/api/v1/ingest/humio-structured')
humio_headers = {'Content-Type': 'application/json',
                 'Authorization': 'Bearer ' + humio_creds['humio_token']}


# A cheap little log printer
def basicLog(message, level='INFO'):
    print("%s [%s] %s"% (datetime.datetime.now(), level, message))


# Initialise the S3 client
client = boto3.client('s3', **aws_creds)

# Get a list of all the objects to process
objects = []
continuationToken = 'UNSET'
while continuationToken:
    if continuationToken == 'UNSET':
        bucketList = client.list_objects_v2(Bucket=args.bucket[0],
                                            Prefix=args.prefix[0])
    else:
        bucketList = client.list_objects_v2(Bucket=args.bucket[0],
                                            Prefix=args.prefix[0],
                                            ContinuationToken=continuationToken)

    if DEBUG: basicLog("Found %d items from bucket listObjectsV2(), includes dirs."% bucketList['KeyCount'], level="DEBUG")

    # This means we have no more keys, or none founc
    if bucketList['KeyCount'] > 0:
        for item in bucketList['Contents']:
            if not item['Key'].endswith('/'):
                objects.append(item['Key'])

    # And here we check to see if there's more results to recover
    if bucketList['IsTruncated']:
        continuationToken = bucketList['NextContinuationToken']
    else:
        continuationToken = False

if not objects:
    basicLog("No Objects Found. If you think there should be then check your prefix.")
    print()
    sys.exit()

basicLog("Found %d items"% len(objects))

# If we haven't found anything then exit at this point
if len(objects) == 0: sys.exit()



# Start processing each file, we're going to be sending SOMETHING so setup the pool
http = urllib3.PoolManager()

arrary_start = b"{\"messageType\":\"DATA_MESSAGE\""

for file in sorted(objects):
    # Create a temp file for the data from S3
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        client.download_fileobj(args.bucket[0], file, temp)
        compressed_temp_filename = temp.name

    with tempfile.TemporaryFile() as temp:
        with gzip.open(compressed_temp_filename, 'rb') as in_f:
            temp.write(in_f.read())

        # Delete the original compressed download
        os.remove(compressed_temp_filename)

        # Avoid loading the whole file into memory
        mm = mmap.mmap(temp.fileno(), 0, prot=mmap.PROT_READ)

        # Start searching at the beginning of the file
        start_search = 0
        message_positions = []

        # Search through the file looking for the message blocks
        while start_search != -1:
            start_search = mm.find(arrary_start, start_search)    
            # If we found another message block then record it, and search again
            if start_search != -1:
                message_positions.append(start_search)
                start_search += 1

        basicLog("Found %d message blocks in %s"% (len(message_positions), file), level="INFO")

        # At this point we start processing the events and sending them to Humio
        # in batches up to 1000 events
        for i, start_pos in enumerate(message_positions):
            # Find the length for this block (i.e. start of next block less one byte)
            try:
                length = (message_positions[i + 1]) - start_pos
            except IndexError:
                # We're looking at the last block
                length = None

            # Load the message blocks and parse as JSON
            mm.seek(start_pos)
            json_data = json.loads(mm.read(length))

            # Format length to "EOF" if it's nonetype
            if not length: length = "EOF"

            if DEBUG: basicLog("Found %d events to process from pos [%d] length [%d] in %s"% (len(json_data['logEvents']), start_pos, length, file), level="DEBUG")

            # The bare payload for Humio structured API request
            payload = [ { "tags": {}, "events": [] } ]
            payload[0]['tags']['logStreamPrefix'] =  '/'.join(json_data['logStream'].split('/')[0:2])
            payload[0]['tags']['logGroup'] = json_data['logGroup']

            hadError = False

            # Process each event to build the payload
            events_to_process = len(json_data['logEvents'])
            for i, event in enumerate(json_data['logEvents']):
                payload[0]['events'].append({ "timestamp": event['timestamp'],
                                                   "attributes": event })

                if (len(payload[0]['events']) == humio_api_batch) or (i == (events_to_process - 1)):
                    ##### SEND HERE #####
                    encoded_data = json.dumps(payload).encode('utf-8')
                    r = http.request('POST', humio_url, body=encoded_data, headers=humio_headers)
                    if r.status == 200:
                        if DEBUG: basicLog("Sent %d events to Humio successfully"% len(payload[0]['events']), level="DEBUG")
                    else:
                        basicLog("Failed to send %d events to Humio, status=%d"% (len(payload[0]['events']), r.status), level="ERROR")
                        hadError = True

                    # Reset the payload
                    payload = [ { "tags": {}, "events": [] } ]
                    payload[0]['tags']['logStreamPrefix'] =  '/'.join(json_data['logStream'].split('/')[0:2])
                    payload[0]['tags']['logGroup'] = json_data['logGroup']


            if hadError:
                basicLog("Error sending some events from file %s"% file, level="ERROR")
            else:
                basicLog("Sent %d events from file %s"% (len(json_data['logEvents']), file), level="INFO")

