// internal/extractor/rpc_client.go
package extractor

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func (c *RPCClient) callRPCEndpoint(payload RPCRequest) (interface{}, error) {
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal RPC request: %w", err)
	}

	req, err := http.NewRequest("POST", c.url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("RPC request failed: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

func (c *RPCClient) GetBlockByNumber(blockNumber uint64) (*models.Block, error) {
	hexBlock := fmt.Sprintf("0x%x", blockNumber)
	payload := RPCRequest{
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{hexBlock, true},
		ID:      1,
		JSONRPC: "2.0",
	}

	result, err := c.callRPCEndpoint(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	jsonData, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block data: %w", err)
	}

	var block models.Block
	if err := json.Unmarshal(jsonData, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block data: %w", err)
	}

	return &block, nil
}
