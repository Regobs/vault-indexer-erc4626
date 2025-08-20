/**
 * Type definitions and interfaces for the ERC4626 transaction indexer
 */

import type { Hex } from 'viem'

/**
 * Configuration interface for the indexer application
 */
export interface IndexerConfig {
  /** RPC endpoint URL for blockchain connection */
  rpcUrl: string
  /** ClickHouse database URL */
  clickhouseUrl: string
  /** ClickHouse authentication credentials */
  clickhouseAuth: string
  /** ClickHouse database name */
  clickhouseDb: string
  /** Blockchain network (mainnet/sepolia) */
  chain: string
  /** ERC4626 vault contract address */
  vault: Hex
  /** Performance tuning parameters */
  performance: {
    /** Number of blocks to process in each batch */
    chunkBlocks: bigint
    /** Maximum range for getLogs calls */
    maxRange: bigint
    /** Minimum range before giving up */
    minRange: bigint
    /** Delay between API calls in milliseconds */
    sleepMs: number
  }
}

/**
 * Parameters for safe log fetching
 */
export interface LogFetchParams {
  /** Contract address(es) to query */
  address: Hex | Hex[]
  /** Event ABI definition (optional - if not provided, fetches all events) */
  event?: any
  /** Starting block number */
  fromBlock: bigint
  /** Ending block number */
  toBlock: bigint
}

/**
 * ERC4626 Deposit event arguments
 */
export interface DepositEventArgs {
  caller: Hex
  owner: Hex
  assets: bigint
  shares: bigint
}

/**
 * ERC4626 Withdrawal event arguments
 */
export interface WithdrawalEventArgs {
  caller: Hex
  receiver: Hex
  owner: Hex
  assets: bigint
  shares: bigint
}

/**
 * ERC20 Transfer event arguments
 */
export interface TransferEventArgs {
  from: Hex
  to: Hex
  value: bigint
}


/**
 * Generic log entry structure
 */
export interface LogEntry {
  blockNumber: bigint
  transactionHash: Hex
  address: Hex
  args: DepositEventArgs | WithdrawalEventArgs | TransferEventArgs
}

/**
 * Database record for deposit events
 */
export interface DepositRecord {
  vault: string
  contractName: string
  sender: string
  owner: string
  assets: string
  shares: string
  blockNumber: string
  txHash: string
  timestamp: string
}

/**
 * Database record for withdrawal events
 */
export interface WithdrawalRecord {
  vault: string
  contractName: string
  sender: string
  receiver: string
  owner: string
  assets: string
  shares: string
  blockNumber: string
  txHash: string
  timestamp: string
}

/**
 * Database record for transfer events
 */
export interface TransferRecord {
  token: string
  contractName: string
  fromAddr: string
  toAddr: string
  value: string
  blockNumber: string
  txHash: string
  timestamp: string
}


/**
 * Indexing coverage record
 */
export interface CoverageRecord {
  address: string
  startBlock: string
  endBlock: string
  status?: 'completed' | 'failed' | 'partial'
  eventsProcessed?: number
  eventsFailed?: number
  updatedAt: string
}