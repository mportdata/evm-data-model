package storage

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type StorageClient struct {
	*minio.Client
}

func NewStorageClient(endpoint, accessKey, secretKey string) (*StorageClient, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return &StorageClient{Client: minioClient}, nil
}

func (c *StorageClient) Exists(path string) (bool, error) {
	_, err := c.StatObject(context.Background(), "staging", path, minio.StatObjectOptions{})
	if err != nil {
		if errResponse, ok := err.(minio.ErrorResponse); ok {
			if errResponse.Code == "NoSuchKey" {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}
