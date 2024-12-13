package main

import (
	"flag"
	"fmt"
	"ingestion-service/internal/ingestion"
	"log"
	"os"
)

func main() {
	// Define command-line arguments
	startBlock := flag.Uint64("start", 0, "Start block number (inclusive)")
	endBlock := flag.Uint64("end", 0, "End block number (exclusive)")
	flag.Parse()

	// Validate command-line arguments
	if *startBlock == 0 || *endBlock == 0 || *endBlock <= *startBlock {
		log.Fatal("Invalid block range. Ensure start and end are set and end > start.")
	}

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

	// Initialize the stager
	stager, err := ingestion.NewStager(rpcEndpoint, minioEndpoint, minioAccessKey, minioSecretKey)
	if err != nil {
		log.Fatalf("Failed to create Stager: %v", err)
	}

	fmt.Printf("Stager successfully initialized: %v\n", stager)

	// Stage blocks in range
	err = stager.StageBlocksInRange(*startBlock, *endBlock)
	if err != nil {
		log.Fatalf("Failed to stage blocks: %v", err)
	}
	fmt.Printf("Successfully staged blocks in range %d to %d\n", *startBlock, *endBlock)
}
