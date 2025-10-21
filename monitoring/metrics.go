package monitoring

import (
	"sync"
	"sync/atomic"
	"time"
)

// MetricsCollector provides comprehensive metrics collection for SignalR operations
type MetricsCollector interface {
	// Connection metrics
	RecordConnection()
	RecordDisconnection()
	RecordConnectionError(transport string, errorType string)

	// Invocation metrics
	RecordInvocation(method string, success bool, duration time.Duration)
	RecordInvocationError(method string, errorType string)

	// Stream metrics
	RecordStreamStart(method string)
	RecordStreamEnd(method string, success bool, duration time.Duration)
	RecordStreamError(method string, errorType string)

	// Transport metrics
	RecordTransportUsage(transport string)
	RecordTransportError(transport string, errorType string)

	// Performance metrics
	RecordMessageSize(bytes int64, direction string)
	RecordLatency(duration time.Duration, operation string)

	// Get current metrics
	GetMetrics() *SignalRMetrics
	ResetMetrics()
}

// SignalRMetrics holds all collected metrics
type SignalRMetrics struct {
	Connections struct {
		Total    int64 `json:"total"`
		Active   int64 `json:"active"`
		Peak     int64 `json:"peak"`
		Rejected int64 `json:"rejected"`
		Errors   int64 `json:"errors"`
	} `json:"connections"`

	Invocations struct {
		Total      int64         `json:"total"`
		Success    int64         `json:"success"`
		Errors     int64         `json:"errors"`
		AvgLatency time.Duration `json:"avg_latency"`
		MaxLatency time.Duration `json:"max_latency"`
	} `json:"invocations"`

	Streams struct {
		Total     int64 `json:"total"`
		Active    int64 `json:"active"`
		Completed int64 `json:"completed"`
		Errors    int64 `json:"errors"`
	} `json:"streams"`

	Transports struct {
		WebSocket        int64 `json:"websocket"`
		ServerSentEvents int64 `json:"sse"`
		LongPolling      int64 `json:"long_polling"`
		Errors           int64 `json:"errors"`
	} `json:"transports"`

	Performance struct {
		TotalMessages      int64 `json:"total_messages"`
		TotalBytesSent     int64 `json:"total_bytes_sent"`
		TotalBytesReceived int64 `json:"total_bytes_received"`
		AvgMessageSize     int64 `json:"avg_message_size"`
	} `json:"performance"`

	Errors struct {
		Total    int64            `json:"total"`
		ByType   map[string]int64 `json:"by_type"`
		ByMethod map[string]int64 `json:"by_method"`
	} `json:"errors"`

	Timestamp time.Time `json:"timestamp"`
}

// DefaultMetricsCollector implements MetricsCollector with thread-safe counters
type DefaultMetricsCollector struct {
	mu sync.RWMutex

	// Connection metrics
	connectionsTotal    int64
	connectionsActive   int64
	connectionsPeak     int64
	connectionsRejected int64
	connectionsErrors   int64

	// Invocation metrics
	invocationsTotal   int64
	invocationsSuccess int64
	invocationsErrors  int64
	invocationsLatency struct {
		total int64
		count int64
		max   int64
	}

	// Stream metrics
	streamsTotal     int64
	streamsActive    int64
	streamsCompleted int64
	streamsErrors    int64

	// Transport metrics
	transports struct {
		websocket   int64
		sse         int64
		longPolling int64
		errors      int64
	}

	// Performance metrics
	performance struct {
		totalMessages      int64
		totalBytesSent     int64
		totalBytesReceived int64
	}

	// Error tracking
	errors struct {
		total    int64
		byType   map[string]int64
		byMethod map[string]int64
	}
}

// NewDefaultMetricsCollector creates a new metrics collector
func NewDefaultMetricsCollector() *DefaultMetricsCollector {
	return &DefaultMetricsCollector{
		errors: struct {
			total    int64
			byType   map[string]int64
			byMethod map[string]int64
		}{
			byType:   make(map[string]int64),
			byMethod: make(map[string]int64),
		},
	}
}

func (m *DefaultMetricsCollector) RecordConnection() {
	atomic.AddInt64(&m.connectionsTotal, 1)
	active := atomic.AddInt64(&m.connectionsActive, 1)

	// Update peak if current active is higher
	for {
		peak := atomic.LoadInt64(&m.connectionsPeak)
		if active <= peak {
			break
		}
		if atomic.CompareAndSwapInt64(&m.connectionsPeak, peak, active) {
			break
		}
	}
}

func (m *DefaultMetricsCollector) RecordDisconnection() {
	atomic.AddInt64(&m.connectionsActive, -1)
}

func (m *DefaultMetricsCollector) RecordConnectionError(transport string, errorType string) {
	atomic.AddInt64(&m.connectionsErrors, 1)
	atomic.AddInt64(&m.transports.errors, 1)

	m.mu.Lock()
	m.errors.byType[errorType]++
	m.errors.total++
	m.mu.Unlock()
}

func (m *DefaultMetricsCollector) RecordInvocation(method string, success bool, duration time.Duration) {
	atomic.AddInt64(&m.invocationsTotal, 1)

	if success {
		atomic.AddInt64(&m.invocationsSuccess, 1)
	} else {
		atomic.AddInt64(&m.invocationsErrors, 1)
	}

	// Update latency metrics
	durationNs := duration.Nanoseconds()
	atomic.AddInt64(&m.invocationsLatency.total, durationNs)
	atomic.AddInt64(&m.invocationsLatency.count, 1)

	// Update max latency
	for {
		max := atomic.LoadInt64(&m.invocationsLatency.max)
		if durationNs <= max {
			break
		}
		if atomic.CompareAndSwapInt64(&m.invocationsLatency.max, max, durationNs) {
			break
		}
	}
}

func (m *DefaultMetricsCollector) RecordInvocationError(method string, errorType string) {
	atomic.AddInt64(&m.invocationsErrors, 1)

	m.mu.Lock()
	m.errors.byType[errorType]++
	m.errors.byMethod[method]++
	m.errors.total++
	m.mu.Unlock()
}

func (m *DefaultMetricsCollector) RecordStreamStart(method string) {
	atomic.AddInt64(&m.streamsTotal, 1)
	atomic.AddInt64(&m.streamsActive, 1)
}

func (m *DefaultMetricsCollector) RecordStreamEnd(method string, success bool, duration time.Duration) {
	atomic.AddInt64(&m.streamsActive, -1)
	atomic.AddInt64(&m.streamsCompleted, 1)

	if !success {
		atomic.AddInt64(&m.streamsErrors, 1)
	}
}

func (m *DefaultMetricsCollector) RecordStreamError(method string, errorType string) {
	atomic.AddInt64(&m.streamsErrors, 1)

	m.mu.Lock()
	m.errors.byType[errorType]++
	m.errors.byMethod[method]++
	m.errors.total++
	m.mu.Unlock()
}

func (m *DefaultMetricsCollector) RecordTransportUsage(transport string) {
	switch transport {
	case "websocket":
		atomic.AddInt64(&m.transports.websocket, 1)
	case "sse":
		atomic.AddInt64(&m.transports.sse, 1)
	case "long_polling":
		atomic.AddInt64(&m.transports.longPolling, 1)
	}
}

func (m *DefaultMetricsCollector) RecordTransportError(transport string, errorType string) {
	atomic.AddInt64(&m.transports.errors, 1)

	m.mu.Lock()
	m.errors.byType[errorType]++
	m.errors.total++
	m.mu.Unlock()
}

func (m *DefaultMetricsCollector) RecordMessageSize(bytes int64, direction string) {
	atomic.AddInt64(&m.performance.totalMessages, 1)

	if direction == "sent" {
		atomic.AddInt64(&m.performance.totalBytesSent, bytes)
	} else {
		atomic.AddInt64(&m.performance.totalBytesReceived, bytes)
	}
}

func (m *DefaultMetricsCollector) RecordLatency(duration time.Duration, operation string) {
	// This could be extended to track latency by operation type
	// For now, we'll use the invocation latency tracking
}

func (m *DefaultMetricsCollector) GetMetrics() *SignalRMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := &SignalRMetrics{
		Timestamp: time.Now(),
	}

	// Connection metrics
	metrics.Connections.Total = atomic.LoadInt64(&m.connectionsTotal)
	metrics.Connections.Active = atomic.LoadInt64(&m.connectionsActive)
	metrics.Connections.Peak = atomic.LoadInt64(&m.connectionsPeak)
	metrics.Connections.Rejected = atomic.LoadInt64(&m.connectionsRejected)
	metrics.Connections.Errors = atomic.LoadInt64(&m.connectionsErrors)

	// Invocation metrics
	metrics.Invocations.Total = atomic.LoadInt64(&m.invocationsTotal)
	metrics.Invocations.Success = atomic.LoadInt64(&m.invocationsSuccess)
	metrics.Invocations.Errors = atomic.LoadInt64(&m.invocationsErrors)

	latencyTotal := atomic.LoadInt64(&m.invocationsLatency.total)
	latencyCount := atomic.LoadInt64(&m.invocationsLatency.count)
	latencyMax := atomic.LoadInt64(&m.invocationsLatency.max)

	if latencyCount > 0 {
		metrics.Invocations.AvgLatency = time.Duration(latencyTotal / latencyCount)
	}
	metrics.Invocations.MaxLatency = time.Duration(latencyMax)

	// Stream metrics
	metrics.Streams.Total = atomic.LoadInt64(&m.streamsTotal)
	metrics.Streams.Active = atomic.LoadInt64(&m.streamsActive)
	metrics.Streams.Completed = atomic.LoadInt64(&m.streamsCompleted)
	metrics.Streams.Errors = atomic.LoadInt64(&m.streamsErrors)

	// Transport metrics
	metrics.Transports.WebSocket = atomic.LoadInt64(&m.transports.websocket)
	metrics.Transports.ServerSentEvents = atomic.LoadInt64(&m.transports.sse)
	metrics.Transports.LongPolling = atomic.LoadInt64(&m.transports.longPolling)
	metrics.Transports.Errors = atomic.LoadInt64(&m.transports.errors)

	// Performance metrics
	metrics.Performance.TotalMessages = atomic.LoadInt64(&m.performance.totalMessages)
	metrics.Performance.TotalBytesSent = atomic.LoadInt64(&m.performance.totalBytesSent)
	metrics.Performance.TotalBytesReceived = atomic.LoadInt64(&m.performance.totalBytesReceived)

	if metrics.Performance.TotalMessages > 0 {
		metrics.Performance.AvgMessageSize = (metrics.Performance.TotalBytesSent + metrics.Performance.TotalBytesReceived) / metrics.Performance.TotalMessages
	}

	// Error metrics
	metrics.Errors.Total = m.errors.total
	metrics.Errors.ByType = make(map[string]int64)
	metrics.Errors.ByMethod = make(map[string]int64)

	for k, v := range m.errors.byType {
		metrics.Errors.ByType[k] = v
	}
	for k, v := range m.errors.byMethod {
		metrics.Errors.ByMethod[k] = v
	}

	return metrics
}

func (m *DefaultMetricsCollector) ResetMetrics() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Reset all atomic counters
	atomic.StoreInt64(&m.connectionsTotal, 0)
	atomic.StoreInt64(&m.connectionsActive, 0)
	atomic.StoreInt64(&m.connectionsPeak, 0)
	atomic.StoreInt64(&m.connectionsRejected, 0)
	atomic.StoreInt64(&m.connectionsErrors, 0)

	atomic.StoreInt64(&m.invocationsTotal, 0)
	atomic.StoreInt64(&m.invocationsSuccess, 0)
	atomic.StoreInt64(&m.invocationsErrors, 0)
	atomic.StoreInt64(&m.invocationsLatency.total, 0)
	atomic.StoreInt64(&m.invocationsLatency.count, 0)
	atomic.StoreInt64(&m.invocationsLatency.max, 0)

	atomic.StoreInt64(&m.streamsTotal, 0)
	atomic.StoreInt64(&m.streamsActive, 0)
	atomic.StoreInt64(&m.streamsCompleted, 0)
	atomic.StoreInt64(&m.streamsErrors, 0)

	atomic.StoreInt64(&m.transports.websocket, 0)
	atomic.StoreInt64(&m.transports.sse, 0)
	atomic.StoreInt64(&m.transports.longPolling, 0)
	atomic.StoreInt64(&m.transports.errors, 0)

	atomic.StoreInt64(&m.performance.totalMessages, 0)
	atomic.StoreInt64(&m.performance.totalBytesSent, 0)
	atomic.StoreInt64(&m.performance.totalBytesReceived, 0)

	// Reset error maps
	m.errors.total = 0
	m.errors.byType = make(map[string]int64)
	m.errors.byMethod = make(map[string]int64)
}

