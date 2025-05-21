package pkg

type IngestionFactory struct {
	DataDir          string
	MaxRetentionTime string
}

func NewIngestionFactory(dir string, maxRetention string) *IngestionFactory {
	return &IngestionFactory{
		DataDir:          dir,
		MaxRetentionTime: maxRetention,
	}
}
