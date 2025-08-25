package operator

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricsNamespace = "glob"

	metricBytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name:      "replica_bytes_received",
		Namespace: metricsNamespace,
		Help:      "The total number of bytes received",
	})

	metricBytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name:      "replica_bytes_sent",
		Namespace: metricsNamespace,
		Help:      "The total number of bytes sent",
	})

	metricReplicaBytesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "replica_bytes_received",
		Namespace: metricsNamespace,
		Help:      "The number of bytes received from a given replica",
	}, []string{"replica"})

	metricReplicaBytesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "replica_bytes_sent",
		Namespace: metricsNamespace,
		Help:      "The number of bytes sent to a given replica",
	}, []string{"replica"})
)
