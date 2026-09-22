package database

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func ListenNotification(
	ctx context.Context,
	db *pgx.Conn,
	channel string,
	handler func(string),
) error {
	_, err := db.Exec(ctx, "LISTEN chat_messages;")
	if err != nil {
		return err
	}

	for {
		notification, err := db.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			handler(notification.Payload)
		}
	}
}
