# EVM Dimensional Data Model

Dimensional data modelling with EVM RPC endpoints.

## Data Models

1. Bronze Layer

   Raw RPC response models

   - [eth_getBalance](models/layer_1_brz/eth_getBalance.md)
   - [eth_getBlockByNumber](models/layer_1_brz/eth_getBlockByNumber.md)
   - [eth_getCode](models/layer_1_brz/eth_getCode.md)
   - [eth_getTransactionReceipt](models/layer_1_brz/eth_getTransactionReceipt.md)

2. [Silver Layer](models/layer_2_slv/README.md)

   - 3NF models
   - Normalized tables

3. [Gold Layer](models/layer_3_gld/README.md)
   - Dimensional models
   - Facts and dimensions
   - Analytics-ready schema

## Project Structure

```python
evm-data-model/
├── src/                     # Implementation code
│   ├── layer_1_brz/
│   ├── layer_2_slv/
│   └── layer_3_gld/
├── models/                  # Data models and documentation
│   ├── layer_1_brz/         # Raw data models
│   ├── layer_2_slv/         # Normalized models
│   └── layer_3_gld/         # Dimensional models
├── tests/                   # Test scripts
└── data/                    # Data storage
│   ├── layer_1_brz/         # Raw data models
│   ├── layer_2_slv/         # Normalized models
│   └── layer_3_gld/
```
