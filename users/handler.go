package users

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"uuid"

	"github.com/raphael-foliveira/pg-backed-jobs/tasks"
)

type TaskHandler struct {
	Enqueuer *Enqueuer
}

func (h *TaskHandler) HandleCreateUserTask(ctx context.Context, task *tasks.Task) error {
	log.Println("creating user...")
	var req CreateUserRequest
	if err := json.Unmarshal(task.Payload, &req); err != nil {
		return fmt.Errorf("failed to unmarshal task payload: %w", err)
	}
	newUser := User{
		ID:        uuid.NewV7().String(),
		Email:     req.Email,
		CreatedAt: time.Now(),
	}
	log.Printf("created user: %+v\n", newUser)
	log.Println("enqueueing send email task...")
	return h.Enqueuer.EnqueueUserEmailTask(ctx, &newUser)
}

func (h *TaskHandler) HandleSendUserEmailTask(ctx context.Context, task *tasks.Task) error {
	log.Println("sending user email...")
	var req EmailTaskPayload
	if err := json.Unmarshal(task.Payload, &req); err != nil {
		return fmt.Errorf("failed to unmarshal task payload: %w", err)
	}
	log.Printf("sent email to user: %+v\n", req)
	return nil
}
