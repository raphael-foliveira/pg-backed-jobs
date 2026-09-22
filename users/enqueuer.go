package users

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/raphael-foliveira/pg-backed-jobs/tasks"
)

const (
	TaskCreateUser    = "create:user"
	TaskSendUserEmail = "send:user_email"
)

type Enqueuer struct {
	Enqueuer *tasks.PGXEnqueuer
}

func (e *Enqueuer) EnqueueCreateUserTask(ctx context.Context, req *CreateUserRequest) error {
	task, err := e.newCreateUserTask(req)
	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}
	return e.Enqueuer.Enqueue(ctx, task)
}

func (e *Enqueuer) EnqueueUserEmailTask(ctx context.Context, user *User) error {
	task, err := e.newUserEmailTask(user)
	if err != nil {
		return err
	}
	return e.Enqueuer.Enqueue(ctx, task)
}

func (e *Enqueuer) newCreateUserTask(req *CreateUserRequest) (*tasks.Task, error) {
	b, err := json.Marshal(req)
	return &tasks.Task{
		Type:    TaskCreateUser,
		Payload: b,
	}, err
}

type EmailTaskPayload struct {
	Email   string `json:"email"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func (e *Enqueuer) newUserEmailTask(user *User) (*tasks.Task, error) {
	b, err := json.Marshal(EmailTaskPayload{
		Email:   user.Email,
		Subject: "Welcome to our test app!",
		Body:    "We are very happy to have you, user!",
	})

	return &tasks.Task{
		Type:    TaskSendUserEmail,
		Payload: b,
	}, err
}
