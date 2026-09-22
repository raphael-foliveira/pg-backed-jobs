package config

import "os"

func EnvOrDefault(envVar, defaultValue string) string {
	val := os.Getenv(envVar)
	if val == "" {
		return defaultValue
	}
	return val
}
