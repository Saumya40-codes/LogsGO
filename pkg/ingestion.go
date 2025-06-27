package pkg

import (
	"log"
	"strconv"
	"time"
	"unicode"
)

type IngestionFactory struct {
	DataDir                 string
	MaxRetentionTime        string
	MaxTimeInMem            string
	UnLockDataDir           bool
	HttpListenAddr          string
	GrpcListenAddr          string
	FlushOnExit             bool
	StoreConfigPath         string
	StoreConfig             string
	WebListenAddr           string
	LookbackPeriod          string
	MemStoreFlushDuration   string
	LocalStoreFlushDuration string
	Insecure                bool
	PublicKeyPath           string
	TLSConfigPath           string
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

func ValidateTimeDurations(dur string) bool {
	if len(dur) < 2 {
		return false
	}

	switch dur[len(dur)-1] {
	case 'd', 'h', 's', 'm':
	default:
		return false
	}

	if ch := rune(dur[len(dur)-2]); !unicode.IsDigit(ch) {
		return false
	}

	return true
}
