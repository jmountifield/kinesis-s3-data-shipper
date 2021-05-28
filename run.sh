export AWS_ACCESS_KEY_ID=XXXXXXXXXXXX
export AWS_SECRET_ACCESS_KEY=YYYYYYYYYYYYYYYYYYYYYYYYYYYYY
export AWS_DEFAULT_REGION=eu-west-1

# Note: client region above must match the sqs queue region below

THREADS=16

seq $THREADS | parallel -n0u \
	python3 cloudtrail-to-humio.py \
	your-s3-bucket-name \
	https://yuo.humio.server/ INGEST_TOKEN \
	--debug \
	--sqs-queue "https://sqs.eu-west-1.amazonaws.com/9876547876/cloudtrail-sqs-queue-name" \
	--continuous \
	--sleep 0
