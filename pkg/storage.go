package pkg

import (
	"log"

	"github.com/dgraph-io/badger/v4"
)

type DB struct {
	conn *badger.DB
}

func OpenDB(path string) *DB {
	opts := badger.DefaultOptions(path).WithLogger(nil).WithBypassLockGuard(true) // just for demo, else there should be a lock!
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal("Failed to open BadgerDB:", err)
	}
	return &DB{conn: db}
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
