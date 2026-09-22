# PostgreSQL-Backed Jobs with River

A small Go example that uses [River](https://riverqueue.com/) for durable PostgreSQL-backed jobs. It creates a user in one job, then enqueues a welcome-email job. The example workers only log their work; they do not persist users or send real email. Email delivery randomly fails to demonstrate retries.

River handles job storage, concurrent claiming, retries, and recovery of jobs interrupted by worker shutdowns. Jobs in this example get up to four total attempts and use River's default retry schedule.

## Running the example

Start PostgreSQL:

```sh
docker compose up -d
```

Run the example application:

```sh
go run .
```

The application applies River's PostgreSQL migrations, registers the two workers, and inserts 150 user-creation jobs. It keeps working until interrupted with Ctrl+C or SIGTERM.

The default development database URL is `postgres://postgres:postgres@localhost:5432/postgres`. Set `DATABASE_URL` to override it.

The old custom queue's `tasks` table is not used by River. Existing rows in that table are not automatically transferred to River's `river_job` table; migrate them separately before retiring the old queue if they contain work that must be completed.

## Project structure

- `main.go` configures the River client, applies migrations, and starts the example.
- `users/` defines River job arguments, insertion methods, and workers.
- `docker-compose.yml` provides a local PostgreSQL instance.
