package pkg

type IngestionFactory struct {
	DataDir          string
	MaxRetentionTime string
	MaxTimeInMem     string
}

func NewIngestionFactory(dir string, maxRetention string, maxTimeInMem string) *IngestionFactory {
	return &IngestionFactory{
		DataDir:          dir,
		MaxRetentionTime: maxRetention,
		MaxTimeInMem:     maxTimeInMem,
	}
}
