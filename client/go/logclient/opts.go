package logclient

import "time"

type Opts struct {
	Service   string
	Level     string
	Message   string
	TimeStamp time.Time
}
