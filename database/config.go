package database

import "github.com/raphael-foliveira/pg-backed-jobs/config"

type Config struct {
	URL string
}

func LoadConfig() (*Config, error) {
	return &Config{
		URL: config.EnvOrDefault(
			"DATABASE_URL",
			"postgres://postgres:postgres@localhost:5432/postgres",
		),
	}, nil
}
