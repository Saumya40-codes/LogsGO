package pkg

import (
	"log"
	"strconv"
	"time"
)

type IngestionFactory struct {
	DataDir          string
	MaxRetentionTime string
	MaxTimeInMem     string
	UnLockDataDir    bool
	HttpListenAddr   string
	FlushOnExit      bool
	StoreConfigPath  string
}

func NewIngestionFactory(dir string, maxRetention string, maxTimeInMem string) *IngestionFactory {
	return &IngestionFactory{
		DataDir:          dir,
		MaxRetentionTime: maxRetention,
		MaxTimeInMem:     maxTimeInMem,
	}
}

func GetTimeDuration(dur string) time.Duration {
	timeDur, err := strconv.Atoi(dur[:(len(dur) - 1)])
	if err != nil {
		log.Fatal("Faced error in duration conversion") // probably this shouldn't happend as cmd/root does checks before hand, helper for testing
	}
	timeDurint64 := time.Duration(timeDur)
	switch dur[(len(dur) - 1)] {
	case 'd':
		return timeDurint64 * 24 * time.Hour
	case 'h':
		return timeDurint64 * time.Hour
	case 'm':
		return timeDurint64 * time.Minute
	case 's':
		return timeDurint64 * time.Second
	default:
		log.Fatal("Invalid time duration specified")
	}

	return 100 * time.Hour // this shouldn't be happening so its ok
}
