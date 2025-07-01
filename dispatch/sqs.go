package dispatch

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Queue wraps an SQS reference
type SQS struct {
	// name string
	url  string
	ctx  context.Context
	conn *sqs.Client
}

// NewQueue initializes the state of the queue struct by fetching the queue URL
// from AWS
func NewSQS(config aws.Config, name string) (q *SQS, err error) {
	conn := sqs.NewFromConfig(config)

	ctx := context.Background()

	resp, err := conn.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		return
	}

	q = &SQS{
		url:  *resp.QueueUrl,
		ctx:  ctx,
		conn: conn,
	}

	return
}

// Send will put a message on the SQS queue
func (q *SQS) Send(message string) error {
	_, err := q.conn.SendMessage(q.ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(message),
		QueueUrl:    &q.url,
	})
	return err
}
