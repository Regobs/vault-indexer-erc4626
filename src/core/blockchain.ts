/**
 * Blockchain utilities for the ERC4626 transaction indexer
 * Handles RPC calls, block timestamp caching, and contract deployment detection
 */

import { createPublicClient, http, parseAbiItem } from 'viem'
import type { Hex, PublicClient } from 'viem'
import { getChain } from '../utils/config.js'
import { logger } from '../utils/logger.js'
import { MemoryAwareCache } from '../storage/cache.js'
import type { IndexerConfig, LogFetchParams } from '../utils/types.js'

/**
 * Sleep utility function for delays
 * @param ms Milliseconds to sleep
 */
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

/**
 * ERC4626 and ERC20 Event ABI Definitions
 */
export const DepositEvt = parseAbiItem('event Deposit(address indexed caller, address indexed owner, uint256 assets, uint256 shares)')
export const WithdrawEvt = parseAbiItem('event Withdraw(address indexed caller, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)')
export const TransferEvt = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 value)')

/**
 * Blockchain client class for handling RPC operations
 */
export class BlockchainClient {
  private client: PublicClient
  private config: IndexerConfig
  private tsCache: MemoryAwareCache<bigint, bigint>
  private log = logger.child('blockchain')

  constructor(config: IndexerConfig) {
    this.config = config
    const chain = getChain(config.chain)
    this.client = createPublicClient({ 
      chain, 
      transport: http(config.rpcUrl) 
    })
    
    // Initialize memory-aware cache with 50MB threshold
    this.tsCache = new MemoryAwareCache<bigint, bigint>(10000, 50)
    
    this.log.verbose('Blockchain client initialized', { 
      chain: config.chain, 
      rpcUrl: config.rpcUrl.substring(0, 50) + '...',
      cacheCapacity: this.tsCache.capacity()
    })
  }

  /**
   * Gets the current block number
   * @returns Latest block number
   */
  async getBlockNumber(): Promise<bigint> {
    const startTime = Date.now()
    try {
      const blockNumber = await this.client.getBlockNumber()
      this.log.rpc('getBlockNumber', undefined, Date.now() - startTime)
      return blockNumber
    } catch (error) {
      this.log.error('Failed to get block number', error)
      throw error
    }
  }

  /**
   * Retrieves block timestamp with caching to reduce RPC calls
   * @param blockNumber Block number to get timestamp for
   * @returns Block timestamp as bigint
   */
  async getBlockTimestamp(blockNumber: bigint): Promise<bigint> {
    const cached = this.tsCache.get(blockNumber)
    if (cached) {
      this.log.verbose(`Cache hit for block ${blockNumber} timestamp`)
      return cached
    }
    
    const startTime = Date.now()
    try {
      const block = await this.client.getBlock({ blockNumber })
      this.tsCache.set(blockNumber, block.timestamp)
      this.log.rpc('getBlock', { blockNumber: blockNumber.toString() }, Date.now() - startTime)
      return block.timestamp
    } catch (error) {
      this.log.error(`Failed to get block ${blockNumber} timestamp`, error)
      throw error
    }
  }

  /**
   * Finds the deployment block of a contract using binary search
   * Returns the block number minus 5 blocks as safety padding
   * @param address Contract address to search for
   * @returns Block number where contract was deployed
   * @throws Error if contract has no code at current block
   */
  async findDeploymentBlock(address: Hex): Promise<bigint> {
    this.log.verbose(`Finding deployment block for contract ${address}`)
    const startTime = Date.now()
    
    const head = await this.client.getBlockNumber()
    const codeNow = await this.client.getCode({ address, blockNumber: head })
    
    if (!codeNow || codeNow === '0x') {
      this.log.error(`No code found at current block for ${address}`)
      throw new Error(`No code at head for ${address} on ${this.config.chain}`)
    }
    
    let lo = 0n, hi = head
    let iterations = 0
    
    while (lo < hi) {
      iterations++
      const mid = (lo + hi) >> 1n
      const code = await this.client.getCode({ address, blockNumber: mid })
      
      if (!code || code === '0x') {
        lo = mid + 1n
      } else {
        hi = mid
      }
      
      if (iterations % 10 === 0) {
        this.log.verbose(`Binary search progress`, { 
          iterations, 
          range: `${lo} - ${hi}`, 
          remaining: Number(hi - lo) 
        })
      }
    }
    
    const deployBlock = lo > 5n ? lo - 5n : 0n
    const duration = Date.now() - startTime
    
    this.log.success(`Found deployment block ${deployBlock} for ${address}`, { 
      iterations, 
      duration: `${duration}ms`,
      paddedBy: 5
    })
    
    return deployBlock
  }

  /**
   * Safely fetches event logs with automatic retry and backoff logic
   * - Splits large block ranges into smaller chunks to avoid provider limits
   * - Handles rate limiting (429 errors) and block range errors with exponential backoff
   * - Progressively reduces range size when encountering limits
   * 
   * @param params Log query parameters including address, event, and block range
   * @param maxRange Maximum blocks per getLogs call
   * @param minRange Minimum blocks before giving up on a range
   * @returns Array of all matching log entries
   */
  async getLogsSafe(
    params: LogFetchParams,
    maxRange?: bigint,
    minRange?: bigint
  ): Promise<any[]> {
    const actualMaxRange = maxRange ?? this.config.performance.maxRange
    const actualMinRange = minRange ?? this.config.performance.minRange
    const { address, event } = params
    
    let from = params.fromBlock
    const to = params.toBlock
    const out: any[] = []
    
    while (from <= to) {
      const end = from + actualMaxRange - 1n > to ? to : from + actualMaxRange - 1n
      let rangeFrom = from, rangeTo = end, range = rangeTo - rangeFrom + 1n
      
      for (;;) {
        try {
          const logsParams: any = { 
            address, 
            fromBlock: rangeFrom, 
            toBlock: rangeTo 
          }
          
          // Only add event filter if provided
          if (event) {
            logsParams.event = event
          }
          
          const logs = await this.client.getLogs(logsParams)
          out.push(...logs)
          break
        } catch (e: any) {
          const msg = `${e?.shortMessage || ''} ${e?.details || ''} ${e?.message || ''}`.toLowerCase()
          const is429 = e?.status === 429 || msg.includes('too many requests') || msg.includes('rate limit')
          const rangeTooWide = msg.includes('block range') || msg.includes('blockrange')
          
          if ((is429 || rangeTooWide) && range > actualMinRange) {
            range = range >> 1n
            rangeTo = rangeFrom + range - 1n
            await sleep(500)
            continue
          }
          
          if (is429) { 
            await sleep(1500)
            continue 
          }
          
          throw e
        }
      }
      
      from = end + 1n
      await sleep(this.config.performance.sleepMs)
    }
    
    return out
  }

  /**
   * Gets contract code at a specific block
   * @param address Contract address
   * @param blockNumber Block number to check
   * @returns Contract bytecode or undefined if no code
   */
  async getCode(address: Hex, blockNumber?: bigint): Promise<Hex | undefined> {
    return await this.client.getCode({ address, blockNumber })
  }

  /**
   * Gets the name of an ERC20/ERC4626 contract
   * @param address Contract address
   * @returns Contract name string
   */
  async getContractName(address: Hex): Promise<string> {
    try {
      const name = await this.client.readContract({
        address,
        abi: [{ 
          name: 'name', 
          type: 'function', 
          inputs: [], 
          outputs: [{ type: 'string' }] 
        }],
        functionName: 'name'
      }) as string
      
      this.log.verbose(`Contract name retrieved: ${name}`, { address })
      return name
      
    } catch (error) {
      this.log.warn(`Failed to get contract name for ${address}`, error)
      return 'Unknown Contract'
    }
  }

  /**
   * Clears the timestamp cache (useful for memory management in long-running processes)
   */
  clearTimestampCache(): void {
    this.tsCache.clear()
    this.log.verbose('Timestamp cache cleared')
  }

  /**
   * Gets the size of the timestamp cache
   * @returns Number of cached timestamps
   */
  getCacheSize(): number {
    return this.tsCache.size()
  }

  /**
   * Gets detailed cache statistics
   * @returns Cache statistics including memory usage
   */
  getCacheStats(): {
    size: number
    capacity: number
    memoryUsage: NodeJS.MemoryUsage
    memoryThresholdMB: number
  } {
    return this.tsCache.getStats()
  }

  /**
   * Cleanup resources on shutdown
   */
  destroy(): void {
    this.tsCache.destroy()
    this.log.info('Blockchain client destroyed')
  }
}