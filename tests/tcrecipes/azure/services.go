package azure

const (
	BlobService  Service = "blob"
	QueueService Service = "queue"
	TableService Service = "table"

	EventHubService = "eventhub"
)

type Service string
