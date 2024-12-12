// cmd/ingest/main.go
package main

import (
	"fmt"
	"ingestion-service/internal/ingestion"
	"log"
	"os"
)

func main() {
	// Get environment variables
	rpcEndpoint := os.Getenv("RPC_ENDPOINT")
	minioEndpoint := os.Getenv("MINIO_ENDPOINT")
	minioAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	minioSecretKey := os.Getenv("MINIO_SECRET_KEY")
	minioBucket := os.Getenv("MINIO_BUCKET")

	// Validate environment variables
	if rpcEndpoint == "" {
		log.Fatal("RPC_ENDPOINT environment variable not set")
	}
	if minioEndpoint == "" || minioAccessKey == "" || minioSecretKey == "" || minioBucket == "" {
		log.Fatal("One or more required MINIO environment variables not set")
	}

	blockIngestor, err := ingestion.NewBlockIngestor(rpcEndpoint, minioEndpoint, minioAccessKey, minioSecretKey)

	if err != nil {
		log.Fatalf("Failed to create block ingestor: %v", err)
	}

	// Stage block
	err = blockIngestor.StageBlock(12345)
	if err != nil {
		log.Fatalf("Failed to get block: %v", err)
	}

	fmt.Printf("Successfully staged block %d\n", 12345)
}
