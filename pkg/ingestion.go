package pkg

type IngestionFactory struct {
	DataDir string
}

func NewIngestionFactory(dir string) *IngestionFactory {
	return &IngestionFactory{
		DataDir: dir,
	}
}
