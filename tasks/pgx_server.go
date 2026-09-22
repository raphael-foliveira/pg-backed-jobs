package tasks

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGXServer struct {
	DB              *pgxpool.Pool
	regLock         sync.RWMutex
	handlerRegistry map[string]HandlerFunc
	maxRetries      int
	backoff         time.Duration
	now             func() time.Time
	leaseInterval   time.Duration
}

func NewPGXServer(db *pgxpool.Pool) *PGXServer {
	return &PGXServer{
		DB:              db,
		handlerRegistry: make(map[string]HandlerFunc),
		maxRetries:      3,
		backoff:         1 * time.Minute,
		now:             time.Now,
		leaseInterval:   10 * time.Minute,
	}
}

func (s *PGXServer) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		recoveryTicker := time.NewTicker(5 * time.Minute)
		defer recoveryTicker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-recoveryTicker.C:
				if err := s.recoverAbandonedTasks(runCtx); err != nil {
					log.Println("failed to recover abandoned tasks:", err)
				}
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := s.retrieveAndHandle(ctx); err != nil {
				return fmt.Errorf("failed to retrieve and handle task: %w", err)
			}
		}
	}
}

func (s *PGXServer) RegisterHandler(taskType string, handler HandlerFunc) {
	s.regLock.Lock()
	defer s.regLock.Unlock()
	if s.handlerRegistry == nil {
		s.handlerRegistry = make(map[string]HandlerFunc)
	}
	s.handlerRegistry[taskType] = handler
	log.Println("registered handler for task type:", taskType)
}

func (s *PGXServer) SetMaxRetries(maxRetries int) *PGXServer {
	s.maxRetries = maxRetries
	return s
}

func (s *PGXServer) SetLeaseInterval(leaseInterval time.Duration) *PGXServer {
	s.leaseInterval = leaseInterval
	return s
}

func (s *PGXServer) SetBackoff(backoff time.Duration) *PGXServer {
	s.backoff = backoff
	return s
}

func (s *PGXServer) SetNowFunc(nowFunc func() time.Time) *PGXServer {
	s.now = nowFunc
	return s
}

func (s *PGXServer) retrieveAndHandle(ctx context.Context) error {
	task, err := s.retrieveTask(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve task: %w", err)
	}
	if task == nil {
		log.Println("no pending tasks found, sleeping...")
		timer := time.NewTimer(1 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
	s.regLock.RLock()
	defer s.regLock.RUnlock()

	if s.handlerRegistry == nil {
		return fmt.Errorf("handler registry is not initialized")
	}

	if handler, ok := s.handlerRegistry[task.Type]; ok {
		if err := handler(ctx, task); err != nil {
			if task.Retries >= s.maxRetries {
				if errFailed := s.markTaskFailed(ctx, task.ID); errFailed != nil {
					return fmt.Errorf("failed to mark task as failed: %w", errFailed)
				}
				return nil
			}
			if errReset := s.setTaskToPending(ctx, task.ID, task.Retries+1); errReset != nil {
				return fmt.Errorf("failed to reset task status: %w", errReset)
			}
			return nil
		}
		if errCompleted := s.markTaskAsCompleted(ctx, task.ID); errCompleted != nil {
			return fmt.Errorf("failed to mark task as completed: %w", errCompleted)
		}
		return nil
	}
	if errDead := s.markTaskAsDead(ctx, task.ID); errDead != nil {
		return fmt.Errorf("failed to mark task as dead: %w", errDead)
	}
	return nil
}

func (s *PGXServer) recoverAbandonedTasks(ctx context.Context) error {
	query := `UPDATE tasks SET 
			status = 'pending', 
			lease_until = NULL, 
			available_at = $1
			WHERE status = 'in progress' AND lease_until <= $1`
	return s.execQuery(ctx, query, s.now())
}

func (s *PGXServer) retrieveTask(ctx context.Context) (*Task, error) {
	now := s.now()
	query := `UPDATE tasks SET status = 'in progress', lease_until = $2 WHERE id = (
			SELECT id FROM tasks WHERE status = 'pending' AND available_at <= $1
			ORDER BY created_at ASC, id ASC
			FOR UPDATE SKIP LOCKED LIMIT 1
		) RETURNING id, type, payload, retries;`
	rows, err := s.DB.Query(ctx, query, now, now.Add(s.leaseInterval))
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve task: %w", err)
	}

	task, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[Task])
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	return task, nil
}

func (s *PGXServer) setTaskToPending(ctx context.Context, taskID int64, retries int) error {
	query := `UPDATE tasks SET status = 'pending', retries = $1, available_at = $2 WHERE id = $3`
	return s.execQuery(ctx, query, retries, s.now().Add(s.backoff), taskID)
}

func (s *PGXServer) markTaskAsCompleted(ctx context.Context, taskID int64) error {
	return s.setTaskStatus(ctx, "completed", taskID)
}

func (s *PGXServer) markTaskAsDead(ctx context.Context, taskID int64) error {
	return s.setTaskStatus(ctx, "dead", taskID)
}

func (s *PGXServer) markTaskFailed(ctx context.Context, taskID int64) error {
	return s.setTaskStatus(ctx, "failed", taskID)
}

func (s *PGXServer) setTaskStatus(ctx context.Context, status string, taskID int64) error {
	query := `UPDATE tasks SET status = $1 WHERE id = $2`
	return s.execQuery(ctx, query, status, taskID)
}

func (s *PGXServer) execQuery(ctx context.Context, query string, args ...any) error {
	_, err := s.DB.Exec(ctx, query, args...)
	return err
}
