/**
 * Configuration management for the ERC4626 transaction indexer
 * Handles environment variable parsing and validation
 */

import 'dotenv/config'
import { getAddress } from 'viem'
import { mainnet, sepolia } from 'viem/chains'
import type { Hex } from 'viem'
import type { IndexerConfig } from './types.js'

/**
 * Loads and validates configuration from environment variables
 * @returns Complete indexer configuration object
 * @throws Error if required environment variables are missing
 */
export function loadConfig(): IndexerConfig {
  const rpcUrl = process.env.RPC_URL
  if (!rpcUrl) {
    throw new Error('RPC_URL environment variable is required')
  }

  const clickhouseUrl = process.env.CLICKHOUSE_URL
  if (!clickhouseUrl) {
    throw new Error('CLICKHOUSE_URL environment variable is required')
  }

  const chainName = (process.env.CHAIN || 'mainnet').toLowerCase()
  const vaultAddress = process.env.VAULT
  
  if (!vaultAddress) {
    throw new Error('VAULT environment variable is required - no default vault address provided for security')
  }

  let vault: Hex
  try {
    vault = getAddress(vaultAddress) as Hex
  } catch (error) {
    throw new Error(`Invalid VAULT address format: ${vaultAddress}. Must be a valid Ethereum address.`)
  }


  return {
    rpcUrl,
    clickhouseUrl,
    clickhouseAuth: process.env.CLICKHOUSE_AUTH || '',
    clickhouseDb: process.env.CLICKHOUSE_DB || 'erc',
    chain: chainName,
    vault,
    performance: {
      chunkBlocks: BigInt(process.env.CHUNK_BLOCKS ?? '5000'),
      maxRange: BigInt(process.env.MAX_RANGE ?? '5000'),
      minRange: BigInt(process.env.MIN_RANGE ?? '512'),
      sleepMs: Number(process.env.SLEEP_MS ?? '900')
    }
  }
}

/**
 * Gets the viem chain object based on configuration
 * @param chainName Network name (mainnet/sepolia)
 * @returns Viem chain configuration
 */
export function getChain(chainName: string) {
  return chainName === 'sepolia' ? sepolia : mainnet
}

/**
 * Validates that all required configuration is present
 * @param config Configuration object to validate
 * @throws Error if configuration is invalid
 */
export function validateConfig(config: IndexerConfig): void {
  if (!config.rpcUrl) {
    throw new Error('RPC URL is required')
  }
  
  if (!config.clickhouseUrl) {
    throw new Error('ClickHouse URL is required')
  }
  
  if (!config.vault) {
    throw new Error('Vault address is required')
  }
  
  if (config.performance.chunkBlocks <= 0n) {
    throw new Error('Chunk blocks must be positive')
  }
  
  if (config.performance.maxRange <= 0n) {
    throw new Error('Max range must be positive')
  }
  
  if (config.performance.minRange <= 0n) {
    throw new Error('Min range must be positive')
  }
  
  if (config.performance.sleepMs < 0) {
    throw new Error('Sleep milliseconds must be non-negative')
  }
}