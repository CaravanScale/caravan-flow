package core

type IngestFn = func(FlowFile) bool

type ConnectorSource interface {
	GetName() string
	GetSourceType() string
	IsRunning() bool
	Start(ingest IngestFn)
	Stop()
	PollOnce() []FlowFile
}

