package pkg

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type DB struct {
	conn *badger.DB
}

func OpenDB(opts badger.Options) (*DB, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badgerDB at %s: %w", opts.Dir, err)
	}
	return &DB{conn: db}, nil
}

func (db *DB) CloseDB() {
	db.conn.Close()
}

func (db *DB) Save(key string, value []byte) error {
	return db.conn.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

func (db *DB) Load(key string) ([]byte, error) {
	var value []byte
	err := db.conn.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}
