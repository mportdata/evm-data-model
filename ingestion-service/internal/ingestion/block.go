// ingestion-service/internal/ingestion/block.go

package ingestion

import (
	"fmt"
	"ingestion-service/internal/extractor"
	"ingestion-service/internal/storage"
)

type BlockIngestor struct {
	rpcClient *extractor.RPCClient
	storage   *storage.StorageClient
}

func NewBlockIngestor(rpcUrl string, storageEndpoint string, storageAccessKey string, storageSecretKey string) (*BlockIngestor, error) {
	rpcClient, err := extractor.NewRPCClient(rpcUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}

	storageClient, err := storage.NewStorageClient(storageEndpoint, storageAccessKey, storageSecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %w", err)
	}

	blockIngestor := &BlockIngestor{
		rpcClient: rpcClient,
		storage:   storageClient,
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}
	return (*BlockIngestor)(blockIngestor), nil
}

func (i *BlockIngestor) StageBlock(blockNum uint64) error {
	// Get block
	block, err := i.rpcClient.GetBlockByNumber(blockNum)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	// Write to staging
	err = i.storage.WriteBlock(blockNum, block)
	if err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	return nil
}
