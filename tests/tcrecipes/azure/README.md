# Testcontainers for Azurite and Event Hubs

## Azurite
The Azurite open-source emulator provides a free local environment for testing your Azure Blob, Queue Storage, and Table Storage applications. 

**Blob Storage** is optimized for storing massive amounts of unstructured data. Unstructured data is data that doesn't adhere to a particular data model or definition, such as text or binary data.

Azure **Queue Storage** is a service for storing large numbers of messages. You access messages from anywhere in the world via authenticated calls using HTTP or HTTPS.

**Table storage** is used to store petabytes of semi-structured data and keep costs down. Unlike many data stores—on-premises or cloud-based—Table storage lets you scale up without having to manually shard your dataset.

Testcontainer reference from [here](https://golang.testcontainers.org/modules/azurite/)
## Event Hubs
Azure Event Hubs is a native data-streaming service in the cloud that can stream millions of events per second, with low latency, from any source to any destination.

Same testcontainer reference was used [here](https://golang.testcontainers.org/modules/azurite/) as well 

## Code:
- *AzuriteContainer* struct represents the Azurite container type used in the module
- *EventHubContainer* struct depends on Azurite container
- type *Service* is a set of constants with types of services
- *withEventHub* writes  [config](https://github.com/Azure/azure-event-hubs-emulator-installer/blob/main/Sample-Code-Snippets/Python/Azure%20Function%20Example/Config.json)