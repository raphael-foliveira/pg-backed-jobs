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
	db                  *pgxpool.Pool
	regLock             sync.RWMutex
	handlerRegistryOnce sync.Once
	handlerRegistry     map[string]HandlerFunc
	maxRetries          int
	backoff             time.Duration
	now                 func() time.Time
	leaseInterval       time.Duration
}

func NewPGXServer(db *pgxpool.Pool) *PGXServer {
	return &PGXServer{
		db:            db,
		maxRetries:    3,
		backoff:       1 * time.Minute,
		now:           time.Now,
		leaseInterval: 10 * time.Minute,
	}
}

func (s *PGXServer) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		if err := s.recoverAbandonedLoop(runCtx); err != nil {
			log.Println(err)
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

func (s *PGXServer) recoverAbandonedLoop(ctx context.Context) error {
	recoveryTicker := time.NewTicker(5 * time.Minute)
	defer recoveryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-recoveryTicker.C:
			if err := s.recoverAbandonedTasks(ctx); err != nil {
				return fmt.Errorf("failed to recover abandoned tasks: %w", err)
			}
		}
	}
}

func (s *PGXServer) RegisterHandler(taskType string, handler HandlerFunc) {
	s.regLock.Lock()
	defer s.regLock.Unlock()
	s.handlerRegistryOnce.Do(func() {
		s.handlerRegistry = make(map[string]HandlerFunc)
	})
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

	if err := s.handleTask(ctx, task); err != nil {
		return err
	}
	return nil
}

func (s *PGXServer) handleTask(ctx context.Context, task *Task) error {
	handler, ok := s.handlerRegistry[task.Type]
	if !ok {
		if errDead := s.markTaskAsDead(ctx, task.ID); errDead != nil {
			return fmt.Errorf("failed to mark task as dead: %w", errDead)
		}
		return nil
	}

	if err := handler(ctx, task); err != nil {
		return s.handleTaskFailure(ctx, task, err)
	}

	if errCompleted := s.markTaskAsCompleted(ctx, task.ID); errCompleted != nil {
		return fmt.Errorf("failed to mark task as completed: %w", errCompleted)
	}
	return nil
}

func (s *PGXServer) handleTaskFailure(ctx context.Context, task *Task, err error) error {
	if task.Retries >= s.maxRetries {
		if errFailed := s.markTaskFailed(ctx, task.ID, err); errFailed != nil {
			return fmt.Errorf("failed to mark task as failed: %w", errFailed)
		}
		return nil
	}
	if errReset := s.setTaskToPending(ctx, task.ID, task.Retries+1); errReset != nil {
		return fmt.Errorf("failed to reset task status: %w", errReset)
	}
	return nil
}

func (s *PGXServer) recoverAbandonedTasks(ctx context.Context) error {
	query := `UPDATE tasks SET 
			status = $3, 
			lease_until = NULL, 
			available_at = $1
			WHERE status = $2 AND lease_until <= $1`
	return s.execQuery(ctx, query, s.now(), StatusInProgress, StatusPending)
}

func (s *PGXServer) retrieveTask(ctx context.Context) (*Task, error) {
	now := s.now()
	query := `UPDATE tasks SET status = $3, lease_until = $2, started_at = $1 WHERE id = (
			SELECT id FROM tasks WHERE status = $4 AND available_at <= $1
			ORDER BY created_at ASC, id ASC LIMIT 1
			FOR UPDATE SKIP LOCKED 
		) RETURNING id, type, payload, retries;`
	rows, err := s.db.Query(ctx, query, now, now.Add(s.leaseInterval), StatusInProgress, StatusPending)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve task: %w", err)
	}

	task, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[Task])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return task, nil
}

func (s *PGXServer) setTaskToPending(ctx context.Context, taskID int64, retries int) error {
	query := `UPDATE tasks SET status = $4, retries = $1, available_at = $2 WHERE id = $3`
	return s.execQuery(ctx, query, retries, s.now().Add(s.backoff), taskID, StatusPending)
}

func (s *PGXServer) markTaskAsCompleted(ctx context.Context, taskID int64) error {
	query := `UPDATE tasks SET status = $1, finished_at = $3 WHERE id = $2`
	return s.execQuery(ctx, query, StatusCompleted, taskID, s.now())
}

func (s *PGXServer) markTaskAsDead(ctx context.Context, taskID int64) error {
	return s.setTaskStatus(ctx, StatusDead, taskID)
}

func (s *PGXServer) markTaskFailed(ctx context.Context, taskID int64, err error) error {
	query := `UPDATE tasks SET status = $1, finished_at = $3, error = $4 WHERE id = $2`
	return s.execQuery(ctx, query, StatusFailed, taskID, s.now(), err.Error())
}

func (s *PGXServer) setTaskStatus(ctx context.Context, status string, taskID int64) error {
	query := `UPDATE tasks SET status = $1 WHERE id = $2`
	return s.execQuery(ctx, query, status, taskID)
}

func (s *PGXServer) execQuery(ctx context.Context, query string, args ...any) error {
	_, err := s.db.Exec(ctx, query, args...)
	return err
}
