package users

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
)

type CreateUserArgs struct {
	Email string `json:"email"`
}

func (CreateUserArgs) Kind() string { return "create_user" }

type SendUserEmailArgs struct {
	Email   string `json:"email"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func (SendUserEmailArgs) Kind() string { return "send_user_email" }

type Enqueuer struct {
	Client *river.Client[pgx.Tx]
}

func (e *Enqueuer) EnqueueCreateUserTask(ctx context.Context, req *CreateUserRequest) error {
	_, err := e.Client.Insert(ctx, CreateUserArgs{Email: req.Email}, nil)
	return err
}

func (e *Enqueuer) EnqueueUserEmailTask(ctx context.Context, user *User) error {
	_, err := e.Client.Insert(ctx, SendUserEmailArgs{
		Email:   user.Email,
		Subject: "Welcome to our test app!",
		Body:    "We are very happy to have you, user!",
	}, nil)
	return err
}

type CreateUserWorker struct {
	river.WorkerDefaults[CreateUserArgs]
	Enqueuer *Enqueuer
}

func (w *CreateUserWorker) Work(ctx context.Context, job *river.Job[CreateUserArgs]) error {
	log.Println("creating user...")
	newUser := User{
		ID:        uuid.NewV7().String(),
		Email:     job.Args.Email,
		CreatedAt: time.Now(),
	}
	log.Printf("created user: %+v", newUser)
	log.Println("enqueueing send email task...")
	if err := w.Enqueuer.EnqueueUserEmailTask(ctx, &newUser); err != nil {
		return fmt.Errorf("enqueue welcome email: %w", err)
	}
	return nil
}

type SendUserEmailWorker struct {
	river.WorkerDefaults[SendUserEmailArgs]
}

func (w *SendUserEmailWorker) Work(_ context.Context, job *river.Job[SendUserEmailArgs]) error {
	log.Println("sending user email...")
	if rand.Intn(10) < 5 {
		return fmt.Errorf("failed to send an email to the user: email not found")
	}
	log.Printf("sent email to user: %+v", job.Args)
	return nil
}
