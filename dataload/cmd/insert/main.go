package main

import (
	"fmt"
	"log"

	"dataload/internal/storage"
)

func main() {
	// Configuration for the storage client
	endpoint := "localhost:9000" // MinIO endpoint
	accessKey := "minioadmin"    // MinIO access key
	secretKey := "minioadmin"    // MinIO secret key
	blockNumber := uint64(339)   // The block number to read

	// Create a new StorageClient
	storageClient, err := storage.NewStorageClient(endpoint, accessKey, secretKey)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}

	// Read the block from storage
	block, err := storageClient.ReadBlock(blockNumber)
	if err != nil {
		log.Fatalf("Failed to read block %d: %v", blockNumber, err)
	}

	// Display the block information
	fmt.Printf("Block %d: %+v\n", blockNumber, block)
}
