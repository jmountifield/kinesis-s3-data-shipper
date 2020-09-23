# kinesis-s3-data-shipper

This data shipper is designed to connect to an AWS S3 bucket, fetch the contents as found at the prefix, and ship the results to Humio. The script expects to find:

 - Files that are either uncompressed, or compressed (once or more) with gzip. No other compression formats are currently supported.
 - Files that have been read from cloudwatch and written to S3 using Kinesis (i.e. kinesis firehose destination from the cloudwatch log group, and S3 target for the firehose with, optionally, gzip compression enabled).

 ## Usage

    % python3 kinesis-to-humio.py --help
    usage: kinesis-to-humio.py [-h] [--prefix PREFIX] [--aws-access-id AWS_ACCESS_ID] [--aws-access-secret AWS_ACCESS_SECRET] [--humio-batch HUMIO_BATCH] [--debug] [--tmpdir TMPDIR] [--track TRACK] bucket humio-host humio-token

    This script is used to coordinate log ingest from S3 where those logs have arrived via an AWS kinesis stream.

    positional arguments:
      bucket                The S3 bucket from which to export. E.g "demo.humio.xyz"
      humio-host            The URL to the target Humio instance, including port number
      humio-token           Ingest token for this input

    optional arguments:
      -h, --help            show this help message and exit
      --prefix PREFIX       The S3 prefix from which to search. E.g "awslogs/2020/04/01/"
      --aws-access-id AWS_ACCESS_ID
                            The AWS access key ID (not implemented)
      --aws-access-secret AWS_ACCESS_SECRET
                            The AWS access key secret (not implemented)
      --humio-batch HUMIO_BATCH
                            max event batch size for Humio API
      --debug               We do the debug?
      --tmpdir TMPDIR       The temp directory where the work will be done
      --track TRACK         A path for a sqlite database for tracking files successfully processed