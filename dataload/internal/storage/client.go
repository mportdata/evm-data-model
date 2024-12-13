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

// writeObject is a generic function to write JSON objects to storage.
func (c *StorageClient) writeObject(objectName string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	_, err = c.PutObject(
		context.Background(),
		"staging",
		objectName,
		bytes.NewReader(jsonData),
		int64(len(jsonData)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		return fmt.Errorf("failed to write object to storage: %w", err)
	}

	return nil
}

// WriteBlock writes a block to storage.
func (c *StorageClient) WriteBlock(blockNum uint64, block *models.Block) error {
	objectName := fmt.Sprintf("blocks/block_%010d.json", blockNum)
	return c.writeObject(objectName, block)
}

// WriteTransactionReceipt writes a transaction receipt to storage.
func (c *StorageClient) WriteTransactionReceipt(txHash string, txReceipt *models.TransactionReceipt) error {
	objectName := fmt.Sprintf("transaction_receipts/tx_receipt_%s.json", txHash)
	return c.writeObject(objectName, txReceipt)
}

// WriteAddressCode writes a transaction receipt to storage.
func (c *StorageClient) WriteAddressCode(address string, addressCode *models.AddressCode) error {
	objectName := fmt.Sprintf("address_codes/address_code_%s.json", address)
	return c.writeObject(objectName, addressCode)
}
