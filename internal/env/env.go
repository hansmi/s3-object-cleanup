package env

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func successOrDie[T any](value T, err error) T {
	if err != nil {
		log.Fatal(err)
	}

	return value
}

func GetWithFallback(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func GetBool(key string, fallback bool) (bool, error) {
	if raw := os.Getenv(key); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return false, fmt.Errorf("environment variable %q: %w", key, err)
		}

		return parsed, nil
	}

	return fallback, nil
}

func MustGetBool(key string, fallback bool) bool {
	return successOrDie(GetBool(key, fallback))
}

func GetDuration(key string, fallback time.Duration) (time.Duration, error) {
	if raw := os.Getenv(key); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("environment variable %q: %s", key, err)
		}

		return parsed, nil
	}

	return fallback, nil
}

func MustGetDuration(key string, fallback time.Duration) time.Duration {
	return successOrDie(GetDuration(key, fallback))
}
