package pkg

import (
	"fmt"
	"log"
	"regexp"

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

func (db *DB) Delete(key string) error {
	err := db.conn.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))

		return err
	})

	return err
}

func (db *DB) GetKey(regex string) ([]string, error) {
	var uniqueKeys []string
	err := db.conn.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if regexp.MustCompile(regex).MatchString(key) {
				uniqueKeys = append(uniqueKeys, key)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get unique keys: %w", err)
	}
	return uniqueKeys, nil
}

func (db *DB) Get(regex string) ([]string, []string, error) {
	var keys []string
	var values []string
	err := db.conn.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if regexp.MustCompile(regex).MatchString(key) {
				keys = append(keys, key)
				value, err := item.ValueCopy(nil)
				if err != nil {
					return fmt.Errorf("failed to get value for key %s: %w", key, err)
				}
				values = append(values, string(value))
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get unique keys: %w", err)
	}
	return keys, values, nil
}

func (db *DB) RunGC() {
	for {
		err := db.conn.RunValueLogGC(0.5)

		if err == badger.ErrNoRewrite {
			break
		}
		if err != nil {
			log.Printf("Badger GC error: %v", err) // TODO: switch to structurred logging
		}
		log.Println("Badger GC: reclaimed space after flush")
	}
}
