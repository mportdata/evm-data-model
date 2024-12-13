package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"dataload/internal/load"
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

	// Validate environment variables
	if rpcEndpoint == "" {
		log.Fatal("RPC_ENDPOINT environment variable not set")
	}
	if minioEndpoint == "" || minioAccessKey == "" || minioSecretKey == "" {
		log.Fatal("One or more required MINIO environment variables not set")
	}

	// Initialize the loader
	loader, err := load.NewLoader(rpcEndpoint, minioEndpoint, minioAccessKey, minioSecretKey)
	if err != nil {
		log.Fatalf("Failed to create Loader: %v", err)
	}

	fmt.Printf("Loader successfully initialized: %v\n", loader)

	// Load blocks in range
	err = loader.LoadBlocksInRange(*startBlock, *endBlock)
	if err != nil {
		log.Fatalf("Failed to load blocks: %v", err)
	}
	fmt.Printf("Successfully loaded blocks in range %d to %d\n", *startBlock, *endBlock)
}
