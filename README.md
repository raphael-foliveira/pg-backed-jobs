# PostgreSQL-Backed Job Queue

A small PostgreSQL-backed job queue written in Go. This project was built by hand as a learning exercise to explore how durable task queues work with PostgreSQL locking, retries, backoff, and task leases.

The queue stores jobs in PostgreSQL and uses `FOR UPDATE SKIP LOCKED` to allow workers to claim pending jobs without claiming the same job concurrently.

## Current features

- PostgreSQL-backed task storage
- Task handlers registered by task type
- Atomic task claiming with `FOR UPDATE SKIP LOCKED`
- FIFO-style ordering by creation time
- Delayed retries through `available_at`
- Configurable maximum retries
- Failed and dead task states
- Task leases through `lease_until`
- Periodic recovery of expired leases
- A small user-creation and welcome-email example

## Task lifecycle

Tasks are inserted as `pending` and become immediately available by default. A worker claims an eligible task and changes it to `in progress` while assigning a lease.

Depending on the handler result, the task is then:

- marked `completed` on success;
- returned to `pending` with a future `available_at` on a retryable failure;
- marked `failed` after the retry limit is reached; or
- marked `dead` when no handler exists for its type.

Expired leases are periodically returned to the pending queue so abandoned tasks can be attempted again.

## Running the example

Start PostgreSQL:

```sh
docker compose up -d
```

Run the example application:

```sh
go run .
```

The application creates the `tasks` table if it does not exist, registers the example handlers, and enqueues sample user-creation tasks. The example handlers log their work rather than persisting users or sending real email.

The default development database configuration is:

```text
postgres://postgres:postgres@localhost:5432/postgres
```

## Project structure

- `tasks/` contains the queue, PostgreSQL access, task claiming, retries, and lease recovery.
- `users/` contains the example task types, payloads, enqueuer, and handlers.
- `main.go` wires the example application together and initializes the database table.
- `docker-compose.yml` provides a local PostgreSQL instance.
