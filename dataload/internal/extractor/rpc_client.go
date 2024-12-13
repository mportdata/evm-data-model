// internal/extractor/rpc_client.go
package extractor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"ingestion-service/internal/models"
)

type RPCClient struct {
	url    string
	client *http.Client
}

func NewRPCClient(url string) (*RPCClient, error) {
	if url == "" {
		return nil, fmt.Errorf("url cannot be empty")
	}
	return &RPCClient{
		url:    url,
		client: &http.Client{},
	}, nil
}

type RPCRequest struct {
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
	JSONRPC string        `json:"jsonrpc"`
}

type RPCResponse struct {
	Result interface{} `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
	ID int `json:"id"`
}

func (c *RPCClient) callRPCEndpoint(method string, params []interface{}, id int, result interface{}) error {
	payload := RPCRequest{
		Method:  method,
		Params:  params,
		ID:      id,
		JSONRPC: "2.0",
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal RPC request: %w", err)
	}

	fmt.Printf("Making request to URL: %s\n", c.url)
	fmt.Printf("Request payload: %s\n", string(jsonPayload))

	req, err := http.NewRequest("POST", c.url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("RPC request failed: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("Response Status: %d\n", resp.StatusCode)

	// Read response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if len(bodyBytes) == 0 {
		return fmt.Errorf("received empty response body")
	}

	// If response is longer than 20 chars, truncate and add ellipsis
	responsePreview := string(bodyBytes)
	if len(responsePreview) > 100 {
		responsePreview = responsePreview[:100] + "..."
	}
	fmt.Printf("Response Body: %s\n", responsePreview)

	// Create new reader for JSON decoder since we consumed the body
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	var rpcResp RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return fmt.Errorf("failed to decode response (body length: %d): %w", len(bodyBytes), err)
	}

	if rpcResp.Error != nil {
		return fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	jsonData, err := json.Marshal(rpcResp.Result)
	if err != nil {
		return fmt.Errorf("failed to marshal result data: %w", err)
	}

	if err := json.Unmarshal(jsonData, result); err != nil {
		return fmt.Errorf("failed to unmarshal result data: %w", err)
	}

	return nil
}

func (c *RPCClient) GetBlockByNumber(blockNumber uint64) (*models.Block, error) {
	hexBlock := fmt.Sprintf("0x%x", blockNumber)
	result := new(models.Block)
	err := c.callRPCEndpoint("eth_getBlockByNumber", []interface{}{hexBlock, true}, 1, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *RPCClient) GetTransactionReceipt(txHash string) (*models.TransactionReceipt, error) {
	result := new(models.TransactionReceipt)
	err := c.callRPCEndpoint("eth_getTransactionReceipt", []interface{}{txHash}, 2, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *RPCClient) GetAddressCode(address string) (*models.AddressCode, error) {
	var result string
	err := c.callRPCEndpoint("eth_getCode", []interface{}{address, "latest"}, 3, &result)
	if err != nil {
		return nil, err
	}
	resultObject := &models.AddressCode{
		Address:     address,
		AddressCode: result,
	}
	return resultObject, nil
}
