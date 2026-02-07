package store

import "time"

type IndexEntry struct {
	MinTS     int64     `json:"min_ts"`
	MaxTS     int64     `json:"max_ts"`
	Level     int       `json:"level"`
	Key       string    `json:"key"`
	CreatedAt time.Time `json:"created_at"`
}

type ServiceIndex struct {
	Entries []IndexEntry `json:"entries"`
}
