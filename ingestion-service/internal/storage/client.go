// internal/storage/client.go
package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"ingestion-service/internal/models"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// StorageClient is a type alias for minio.Client
type StorageClient minio.Client

func NewStorageClient(endpoint, accessKey, secretKey string) (*StorageClient, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return (*StorageClient)(minioClient), nil
}

func (c *StorageClient) WriteBlock(blockNum uint64, block *models.Block) error {
	jsonData, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block: %w", err)
	}

	objectName := fmt.Sprintf("blocks/block_%010d.json", blockNum)

	_, err = (*minio.Client)(c).PutObject(
		context.Background(),
		"staging",
		objectName,
		bytes.NewReader(jsonData),
		int64(len(jsonData)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		return fmt.Errorf("failed to write block to storage: %w", err)
	}

	return nil
}
