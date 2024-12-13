package load

import (
	"dataload/internal/extract"
	"dataload/internal/storage"
	"fmt"
	"runtime"
	"sync"
)

// RelationalHandler handles interrelated requests for staging data
type Loader struct {
	RPCClient     *extract.RPCClient
	StorageClient *storage.StorageClient
}

func NewLoader(rpcEndpoint string, minioEndpoint string, minioAccessKey string, minioSecretKey string) (*Loader, error) {
	// Create RPC client
	rpcClient, err := extract.NewRPCClient(rpcEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to establish new RPC client: %w", err)
	}

	// Create Storage client
	storageClient, err := storage.NewStorageClient(minioEndpoint, minioAccessKey, minioSecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to establish new Storage client: %w", err)
	}

	// Return the new Loader instance
	return &Loader{
		RPCClient:     rpcClient,
		StorageClient: storageClient,
	}, nil
}

func (l *Loader) LoadBlockAndRelatedData(blockNum uint64) error {
	// Check if block already exists in storage
	blockFileName := fmt.Sprintf("blocks/block_%010d.json", blockNum)
	exists, err := l.StorageClient.Exists(blockFileName)
	if err != nil {
		return fmt.Errorf("failed to check existence of block file %s: %w", blockFileName, err)
	}

	if exists {
		// Skip processing if block already exists
		fmt.Printf("Block %d already loaded. Skipping...\n", blockNum)
		return nil
	}

	// Fetch block
	block, err := l.RPCClient.GetBlockByNumber(blockNum)
	if err != nil {
		return fmt.Errorf("failed to fetch block %d: %w", blockNum, err)
	}

	// Write block to storage
	if err := l.StorageClient.WriteBlock(blockNum, block); err != nil {
		return fmt.Errorf("failed to write block %d: %w", blockNum, err)
	}

	// Fetch and load related transactions and receipts
	transactionHashes := block.GetTransactionHashes()
	for _, txHash := range transactionHashes {
		// Fetch transaction receipt
		receipt, err := l.RPCClient.GetTransactionReceipt(txHash)
		if err != nil {
			return fmt.Errorf("failed to fetch receipt for tx %s: %w", txHash, err)
		}

		// Write transaction receipt to storage
		if err := l.StorageClient.WriteTransactionReceipt(txHash, receipt); err != nil {
			return fmt.Errorf("failed to write receipt for tx %s: %w", txHash, err)
		}

		// Fetch and load related addresses
		addresses := receipt.GetAddresses()
		for _, address := range addresses {
			addressCode, err := l.RPCClient.GetAddressCode(address)
			if err != nil {
				return fmt.Errorf("failed to fetch address code for address %s: %w", address, err)
			}

			// Write address code to storage
			if err := l.StorageClient.WriteAddressCode(address, addressCode); err != nil {
				return fmt.Errorf("failed to write address code for address %s: %w", address, err)
			}
		}
	}

	return nil
}

func (l *Loader) LoadBlocksInRange(startBlock, endBlock uint64) error {
	// Validate block range
	if startBlock > endBlock {
		return fmt.Errorf("invalid block range: startBlock (%d) is greater than endBlock (%d)", startBlock, endBlock)
	}

	fmt.Printf("Starting full block load for range: %d to %d\n", startBlock, endBlock)

	// Create a worker pool to process blocks concurrently
	blockChan := make(chan uint64, endBlock-startBlock+1)
	errorChan := make(chan error, endBlock-startBlock+1)

	workerCount := runtime.NumCPU()
	var wg sync.WaitGroup

	// Launch worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blockNum := range blockChan {
				if err := l.LoadBlockAndRelatedData(blockNum); err != nil {
					errorChan <- fmt.Errorf("block %d: %w", blockNum, err)
				}
			}
		}()
	}

	// Send block numbers to the channel
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		blockChan <- blockNum
	}
	close(blockChan)

	// Wait for all workers to finish
	wg.Wait()
	close(errorChan)

	// Check for errors
	var hasErrors bool
	for err := range errorChan {
		hasErrors = true
		fmt.Printf("Error: %v\n", err)
	}

	if hasErrors {
		return fmt.Errorf("some blocks failed to load; check logs for details")
	}

	fmt.Printf("Successfully completed full block load for range: %d to %d\n", startBlock, endBlock)
	return nil
}
