# src/layer_2_silver/models.py
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Block_Datatype:
    hash: str
    parent_hash: str
    number: int
    timestamp: datetime
    miner: str
    gas_limit: int
    gas_used: int
    base_fee_per_gas: int
    size: int
    difficulty: int


@dataclass
class BlockRoots:
    block_hash: str
    receipts_root: str
    state_root: str
    transactions_root: str


@dataclass
class BlockConsensus:
    block_hash: str
    nonce: str
    mix_hash: str
    sha3_uncles: str


@dataclass
class BlockMetadata:
    block_hash: str
    logs_bloom: str
    extra_data: str


@dataclass
class BlockRunningTotals:
    block_hash: str
    total_difficulty: int
    cumulative_gas_used: int
