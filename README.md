# EVM Dimensional Data Model

Dimensional data modelling with EVM RPC endpoints.

## Data Models

1. Bronze Layer

   Raw RPC response models

   - [eth_getBalance](models/01_bronze/eth_getBalance.md)
   - [eth_getBlockByNumber](models/01_bronze/eth_getBlockByNumber.md)
   - [eth_getCode](models/01_bronze/eth_getCode.md)
   - [eth_getTransactionReceipt](models/01_bronze/eth_getTransactionReceipt.md)

2. [Silver Layer](models/02_silver/README.md)

   - 3NF models
   - Normalized tables

3. [Gold Layer](models/03_gold/README.md)
   - Dimensional models
   - Facts and dimensions
   - Analytics-ready schema

## Project Structure

```python
evm-data-model/
├── src/                    # Implementation code
├── models/                 # Data models and documentation
│   ├── 01_bronze/         # Raw data models
│   ├── 02_silver/         # Normalized models
│   └── 03_gold/           # Dimensional models
├── tests/                 # Test scripts
└── data/                  # Data storage
```
