package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	LogsIngested        *prometheus.CounterVec
	BucketCalls         prometheus.Counter
	LogsFlushed         *prometheus.CounterVec
	CurrentLogsIngested *prometheus.GaugeVec
	IngestionDuration   *prometheus.HistogramVec
	FlushDuration       *prometheus.HistogramVec
}

// NewMetrics initializes and returns a Metrics struct with the necessary Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		LogsIngested: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "logs_ingested_total",
			Help: "Total number of logs ingested, across various stored",
		}, []string{"store"}),
		BucketCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bucket_calls_total",
			Help: "Total number of calls to the bucket API",
		}),
		LogsFlushed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "logs_flushed_total",
			Help: "Total number of logs flushed, across the stores",
		}, []string{"from", "to"}),
		CurrentLogsIngested: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "current_logs_ingested",
			Help: "Current number of logs hold across all stores",
		}, []string{"store"}),
		IngestionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:                           "ingestion_duration_seconds",
			Help:                           "Duration of log ingestion in seconds",
			NativeHistogramBucketFactor:    2,   // corresponds to ratio where schema will be 2^(2^0) = 2, for e.g., 1s, 2s, 4s, 8s, etc.
			NativeHistogramMaxBucketNumber: 100, // after this,it will decrease the resolution, thus schema decreases (formula is 2^(2^(-schema)))
		}, []string{"store"}),
		FlushDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:                           "ingestion_duration_seconds",
			Help:                           "Duration of log ingestion in seconds",
			NativeHistogramBucketFactor:    2,
			NativeHistogramMaxBucketNumber: 100,
		}, []string{"from", "to"}),
	}
}

// RegisterMetrics registers the metrics with Prometheus.
func (m *Metrics) RegisterMetrics(reg *prometheus.Registry) error {
	if err := reg.Register(m.LogsIngested); err != nil {
		return err
	}
	if err := reg.Register(m.BucketCalls); err != nil {
		return err
	}
	if err := reg.Register(m.LogsFlushed); err != nil {
		return err
	}
	if err := reg.Register(m.CurrentLogsIngested); err != nil {
		return err
	}
	if err := reg.Register(m.IngestionDuration); err != nil {
		return err
	}
	return nil
}
