package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	LogsIngested        *prometheus.CounterVec
	BucketCalls         prometheus.Counter
	LogsFlushed         *prometheus.CounterVec
	CurrentLogsIngested *prometheus.GaugeVec
	IngestionDuration   *prometheus.HistogramVec
	FlushDuration       *prometheus.HistogramVec
	QueryDuration       *prometheus.HistogramVec
	QueryErrors         *prometheus.CounterVec
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
			NativeHistogramBucketFactor:    2,
			NativeHistogramMaxBucketNumber: 100,
		}, []string{"store"}),
		// Was incorrectly named ingestion_duration_seconds and never registered,
		// so flush latency was invisible under load.
		FlushDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:                           "flush_duration_seconds",
			Help:                           "Duration of store flush operations in seconds",
			NativeHistogramBucketFactor:    2,
			NativeHistogramMaxBucketNumber: 100,
		}, []string{"from", "to"}),
		QueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:                           "query_duration_seconds",
			Help:                           "Duration of log queries in seconds",
			NativeHistogramBucketFactor:    2,
			NativeHistogramMaxBucketNumber: 100,
		}, []string{"type"}),
		QueryErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "query_errors_total",
			Help: "Total number of failed queries by reason",
		}, []string{"reason"}),
	}
}

// RegisterMetrics registers the metrics with Prometheus.
func (m *Metrics) RegisterMetrics(reg *prometheus.Registry) error {
	collectors := []prometheus.Collector{
		m.LogsIngested,
		m.BucketCalls,
		m.LogsFlushed,
		m.CurrentLogsIngested,
		m.IngestionDuration,
		m.FlushDuration,
		m.QueryDuration,
		m.QueryErrors,
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}
