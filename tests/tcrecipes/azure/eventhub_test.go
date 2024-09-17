package azure

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainer(t *testing.T) {

	azuriteContainer, err := RunAzurite(context.Background(), "mcr.microsoft.com/azure-storage/azurite:latest")
	require.NoError(t, err)
	azuriteService, err := azuriteContainer.ServiceURL(context.Background(), BlobService)
	require.NoError(t, err)
	require.NotEmpty(t, azuriteService)
	name, err := azuriteContainer.Name(context.Background())
	if err != nil {
		require.NoError(t, err)
	}

	eventhubContainer, err := RunEventHub(context.Background(), "mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest", trimFirstRune(name))
	require.NoError(t, err)
	eventhubService, err := eventhubContainer.ServiceURL(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, eventhubService)
}

func trimFirstRune(s string) string {
	return string([]rune(s)[1:])
}
