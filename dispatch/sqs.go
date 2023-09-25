package dispatch

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Queue wraps an SQS reference
type Queue struct {
	// name string
	url  string
	conn *sqs.SQS
}

// NewQueue initializes the state of the queue struct by fetching the queue URL
// from AWS
func NewQueue(config *aws.Config, name string) (q *Queue, err error) {
	var sn *session.Session
	if sn, err = session.NewSession(config); err != nil {
		return
	}
	conn := sqs.New(sn)

	qInput := &sqs.GetQueueUrlInput{}
	qInput.SetQueueName(name)
	resp, err := conn.GetQueueUrl(qInput)
	if err != nil {
		return
	}

	q = &Queue{
		url:  *resp.QueueUrl,
		conn: conn,
	}

	return
}

// Send will put a message on the SQS queue
func (q *Queue) Send(message string) error {
	_, err := q.conn.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(message),
		QueueUrl:    &q.url,
	})
	return err
}
