// internal/models/receipt.go
package models

type TransactionReceipt struct {
	BlockHash         string `json:"blockHash"`
	BlockNumber       string `json:"blockNumber"`
	ContractAddress   string `json:"contractAddress"`
	CumulativeGasUsed string `json:"cumulativeGasUsed"`
	DepositNonce      string `json:"depositNonce"`
	EffectiveGasPrice string `json:"effectiveGasPrice"`
	From              string `json:"from"`
	GasUsed           string `json:"gasUsed"`
	Logs              []Log  `json:"logs"`
	LogsBloom         string `json:"logsBloom"`
	Status            string `json:"status"`
	To                string `json:"to"`
	TransactionHash   string `json:"transactionHash"`
	TransactionIndex  string `json:"transactionIndex"`
	Type              string `json:"type"`
}

// GetAddresses returns the to and from addresses from a transaction receipt
func (t *TransactionReceipt) GetAddresses() []string {
	addresses := make([]string, 0, 2) // Preallocate for up to 2 addresses

	if t.To != "" { // Append only if To is non-empty
		addresses = append(addresses, t.To)
	}
	if t.From != "" && t.From != t.To { // Append From if non-empty and distinct from To
		addresses = append(addresses, t.From)
	} else if t.From != "" { // Append From even if it's the same as To
		addresses = append(addresses, t.From)
	}

	return addresses
}
