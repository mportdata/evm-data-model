package storage

import (
	"bytes"
	"context"
	"dataload/internal/models"
	"encoding/json"
	"fmt"

	"github.com/minio/minio-go/v7"
)

// writeObject is a generic function to write JSON objects to storage.
func (c *StorageClient) writeObject(objectName string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	_, err = c.PutObject(
		context.Background(),
		"raw",
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

// readObject is a generic function to read JSON objects from storage.
func (c *StorageClient) readObject(bucket string, objectName string, v interface{}) error {
	// Retrieve the object
	object, err := c.GetObject(context.Background(), bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object %s from bucket %s: %w", objectName, bucket, err)
	}
	defer object.Close()

	// Decode JSON into the provided struct
	if err := json.NewDecoder(object).Decode(v); err != nil {
		return fmt.Errorf("failed to decode object %s: %w", objectName, err)
	}

	return nil
}

func (c *StorageClient) ReadBlock(blockNumber uint64) (*models.Block, error) {
	objectName := fmt.Sprintf("blocks/block_%010d.json", blockNumber)
	var block models.Block
	if err := c.readObject("raw", objectName, &block); err != nil {
		return nil, fmt.Errorf("failed to read block %d: %w", blockNumber, err)
	}
	return &block, nil
}

func (c *StorageClient) ReadTransactionReceipt(txHash string) (*models.TransactionReceipt, error) {
	objectName := fmt.Sprintf("transaction_receipts/tx_receipt_%s.json", txHash)
	var txReceipt models.TransactionReceipt
	if err := c.readObject("raw", objectName, &txReceipt); err != nil {
		return nil, fmt.Errorf("failed to read transaction receipt %s: %w", txHash, err)
	}
	return &txReceipt, nil
}

func (c *StorageClient) ReadAddressCode(address string) (*models.AddressCode, error) {
	objectName := fmt.Sprintf("address_codes/address_code_%s.json", address)
	var addressCode models.AddressCode
	if err := c.readObject("raw", objectName, &addressCode); err != nil {
		return nil, fmt.Errorf("failed to read adress code %s: %w", address, err)
	}
	return &addressCode, nil
}
