// internal/models/transaction.go
package models

type Transaction struct {
	BlockHash             string `json:"blockHash"`
	BlockNumber           string `json:"blockNumber"`
	From                  string `json:"from"`
	Gas                   string `json:"gas"`
	GasPrice              string `json:"gasPrice"`
	Hash                  string `json:"hash"`
	Input                 string `json:"input"`
	Nonce                 string `json:"nonce"`
	To                    string `json:"to"`
	TransactionIndex      string `json:"transactionIndex"`
	Value                 string `json:"value"`
	Type                  string `json:"type"`
	V                     string `json:"v"`
	R                     string `json:"r"`
	S                     string `json:"s"`
	SourceHash            string `json:"sourceHash,omitempty"`
	Mint                  string `json:"mint,omitempty"`
	DepositReceiptVersion string `json:"depositReceiptVersion,omitempty"`
}
