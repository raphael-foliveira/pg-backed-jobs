package tasks

import "context"

type (
	Task struct {
		ID      int64  `db:"id"`
		Type    string `db:"type"`
		Payload []byte `db:"payload"`
		Retries int    `db:"retries"`
	}

	HandlerFunc func(ctx context.Context, task *Task) error
)
