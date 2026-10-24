#!/bin/sh
set -eu

aws_sqs() {
  aws --endpoint-url "$LOCALSTACK_ENDPOINT_URL" --region "$AWS_REGION" sqs "$@"
}

case "$SQS_MAX_RECEIVE_COUNT" in
  ''|*[!0-9]*|0)
    echo "SQS_MAX_RECEIVE_COUNT must be a positive integer" >&2
    exit 1
    ;;
esac

attempt=0
until aws_sqs list-queues >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    echo "Unable to reach SQS at $LOCALSTACK_ENDPOINT_URL after 30 attempts" >&2
    aws_sqs list-queues >&2
    exit 1
  fi
  echo "Waiting for SQS at $LOCALSTACK_ENDPOINT_URL"
  sleep 2
done

dlq_url="$(
  aws_sqs create-queue \
    --queue-name "$SQS_DLQ_QUEUE_NAME" \
    --attributes FifoQueue=true,ContentBasedDeduplication=true \
    --query QueueUrl \
    --output text
)"
dlq_arn="$(
  aws_sqs get-queue-attributes \
    --queue-url "$dlq_url" \
    --attribute-names QueueArn \
    --query Attributes.QueueArn \
    --output text
)"

events_url="$(
  aws_sqs create-queue \
    --queue-name "$SQS_EVENTS_QUEUE_NAME" \
    --attributes FifoQueue=true,ContentBasedDeduplication=true \
    --query QueueUrl \
    --output text
)"

attributes_file="$(mktemp)"
trap 'rm -f "$attributes_file"' EXIT
printf '{"FifoQueue":"true","ContentBasedDeduplication":"true","RedrivePolicy":"{\\"deadLetterTargetArn\\":\\"%s\\",\\"maxReceiveCount\\":\\"%s\\"}"}' \
  "$dlq_arn" "$SQS_MAX_RECEIVE_COUNT" > "$attributes_file"

main_url="$(
  aws_sqs create-queue \
    --queue-name "$SQS_MAIN_QUEUE_NAME" \
    --attributes "file://$attributes_file" \
    --query QueueUrl \
    --output text
)"

echo "SQS FIFO queues ready: $SQS_MAIN_QUEUE_NAME ($main_url), $SQS_DLQ_QUEUE_NAME ($dlq_url), $SQS_EVENTS_QUEUE_NAME ($events_url)"
