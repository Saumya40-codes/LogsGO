package pkg

import (
	"fmt"
	"log"

	"github.com/cockroachdb/pebble/v2"
)

// NoopLogger silences pebble's info logs while keeping errors visible
type NoopLogger struct{}

func (NoopLogger) Infof(format string, args ...interface{}) {}

func (NoopLogger) Errorf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (NoopLogger) Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}

type DB struct {
	Conn   *pebble.DB
	closed bool
}

func OpenDB(dir string, opts *pebble.Options) (*DB, error) {
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble at %s: %w", dir, err)
	}
	return &DB{Conn: db}, nil
}

func (db *DB) CloseDB() error {
	if db.closed {
		return nil
	}
	db.closed = true
	return db.Conn.Close()
}

func (db *DB) IsClosed() bool {
	return db.closed
}
