import boto3
import sys
import datetime
import tempfile
import gzip
import mmap
import os
import json
import urllib3
import urllib.parse
import shutil


# Bytes string that identifies the start of the array
arrary_start = b"{\"messageType\":\"DATA_MESSAGE\""


def humioUrl(args):
    return urllib.parse.urljoin(args['humio-host'], '/api/v1/ingest/humio-structured')

def humioHeaders(args):
    # Humio structured api endpoint
    return {'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + args['humio-token']}


# A cheap little log printer
def basicLog(message, level='INFO'):
    print("%s [%s] %s"% (datetime.datetime.now(), level, message))


def isCompressed(file):
    """Detects if a file at a specific path is gzip compressed."""
    try:
        gzip.GzipFile(filename=file).peek(64)
        return True
    except OSError as e:
        return False


def parseAndSend(args, file, http, client):

    # We're going to process each file in a dedicated temp directory
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Get the filename from the file path
        filePath = os.path.join(tmpdirname, os.path.basename(file))

        # Download the source file from S3
        client.download_file(args['bucket'], file, filePath)

        # Now we may need to decompress the file, and potentially even TWICE, eurgh
        while isCompressed(filePath):
            # Decompress the file in-place
            with gzip.open(filePath, 'rb') as f_in, open(filePath + ".working", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            # Move the new file over the top of the original
            shutil.move(filePath + ".working", filePath)

        # Open the new uncompressed version of the download and mmap it (it could be big!)
        with open(filePath) as cloudwatchEvents:
            mm = mmap.mmap(cloudwatchEvents.fileno(), 0, prot=mmap.PROT_READ)

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

            # sys.exit()

            # At this point we start processing the events and sending them to Humio
            # in batches up to humio-batch events
            for i, start_pos in enumerate(message_positions):
                # Find the length for this block (i.e. start of next block less one byte)
                try:
                    length = (message_positions[i + 1]) - start_pos
                except IndexError:
                    # We're looking at the last block
                    length = -1

                # Load the message blocks and parse as JSON
                mm.seek(start_pos)
                json_data = json.loads(mm.read(length))

                if args['debug']: basicLog("Found %d events to process from pos [%d] length [%d] in %s"% (len(json_data['logEvents']), start_pos, length, file), level="DEBUG")

                # The bare payload for Humio structured API request
                payload = [ { "tags": {}, "events": [] } ]
                payload[0]['tags']['logStreamPrefix'] =  '/'.join(json_data['logStream'].split('/')[0:2])
                payload[0]['tags']['logGroup'] = json_data['logGroup']

                hadError = False

                # Process each event to build the payload
                events_to_process = len(json_data['logEvents'])
                for i, event in enumerate(json_data['logEvents']):
                    event['file'] = file
                    payload[0]['events'].append({ "timestamp": event['timestamp'],
                                                  "attributes": event })

                    if (len(payload[0]['events']) == args['humio_batch']) or (i == (events_to_process - 1)):
                        ##### SEND HERE #####
                        encoded_data = json.dumps(payload).encode('utf-8')
                        r = http.request('POST', humioUrl(args), body=encoded_data, headers=humioHeaders(args))
                        if r.status == 200:
                            if args['debug']: basicLog("Sent %d events to Humio successfully"% len(payload[0]['events']), level="DEBUG")
                        else:
                            basicLog("Failed to send %d events to Humio, status=%d"% (len(payload[0]['events']), r.status), level="ERROR")
                            hadError = True

                        # Reset the payload
                        payload = [ { "tags": {}, "events": [] } ]
                        # We're only sending one group of events, so set the tags on the first set
                        payload[0]['tags']['logStreamPrefix'] =  '/'.join(json_data['logStream'].split('/')[0:2])
                        payload[0]['tags']['logGroup'] = json_data['logGroup']


                if hadError:
                    basicLog("Error sending some events from file %s"% filePath, level="ERROR")
                else:
                    basicLog("Sent %d events from file %s"% (len(json_data['logEvents']), file), level="INFO")



def findFiles(args, client):
    # Get a list of all the objects to process
    objects = []
    continuationToken = 'UNSET'
    while continuationToken:
        if continuationToken == 'UNSET':
            bucketList = client.list_objects_v2(Bucket=args['bucket'],Prefix=args['prefix'])
        else:
            bucketList = client.list_objects_v2(Bucket=args['bucket'], Prefix=args['prefix'], ContinuationToken=continuationToken)

        if args['debug']: basicLog("Found %d items from bucket list_objects_v2(), includes dirs."% bucketList['KeyCount'], level="DEBUG")

        # This means we have no more keys, or none found
        if bucketList['KeyCount'] > 0:
            for item in bucketList['Contents']:
                if not item['Key'].endswith('/'): # ignore directories
                    objects.append(item['Key'])

        # And here we check to see if there's more results to recover
        if bucketList['IsTruncated']:
            continuationToken = bucketList['NextContinuationToken']
        else:
            continuationToken = False

    # What did we get?
    basicLog("Found %d items"% len(objects))

    return objects



def prettyPrintArgs(args):
    print("Running with the following arguments:")
    print()
    for arg in args:
        argNamePadded = "{:<18}".format(arg)
        if arg in ['aws-access-secret', 'humio-token']:
            print("\t%s =>\t%s"% (argNamePadded, str("*" * len(str(args[arg])))))
        else:
            print("\t%s =>\t%s"% (argNamePadded, str(args[arg])))
    print()


if __name__ == "__main__":
    
    # We only need to do the argparse if we're running interactivley
    import argparse

    parser = argparse.ArgumentParser(description='This script is used to coordinate log ingest from S3 where those logs have arrived via an AWS kinesis stream.')

    # Details for the source bucket and access
    parser.add_argument('bucket',            type=str, action='store', help='The S3 bucket from which to export. E.g "demo.humio.xyz"')
    parser.add_argument('--prefix',          type=str, action='store', default="", help='The S3 prefix from which to search. E.g "awslogs/2020/04/01/"')
    parser.add_argument('aws-access-id',     type=str, action='store', help='The AWS access key ID')
    parser.add_argument('aws-access-secret', type=str, action='store', help='The AWS access key secret')

    # Target system where the logs will be sent
    parser.add_argument('humio-host',        type=str, action='store', default="https://cloud.humio.com/", help='The URL to the target Humio instance, including port number')
    parser.add_argument('humio-token',       type=str, action='store', help='Ingest token for this input')
    parser.add_argument('--humio-batch',     type=int, action='store', default=5000, help='max event batch size for Humio API')

    # Are we going to do the debug?
    parser.add_argument('--debug',           action='store_true', help='We do the debug?')

    # Build the argument list
    args = vars(parser.parse_args())

    # Echo to stdout so we can see the args in use if debug
    if args['debug']: prettyPrintArgs(args)

    # Initialise the S3 client
    client = boto3.client('s3')

    # Find the files to download and process
    filesToProcess = findFiles(args, client)

    if not filesToProcess:
        basicLog("No Objects Found. If you think there should be then check yo' prefix.")
        sys.exit()

    # We're going to start sending some events so setup the http connection pool
    http = urllib3.PoolManager()

    # Process each file that we want to send to Humio
    for file in sorted(filesToProcess):
        parseAndSend(args, file, http, client)


    # print(args)
    sys.exit()

