/**
 * ClickHouse database client utilities
 * Provides centralized database operations for the ERC4626 indexer
 */

import type { IndexerConfig } from './types.js'

/**
 * Executes a ClickHouse query with JSON response
 * @param config Indexer configuration
 * @param sql SQL query to execute
 * @returns Parsed JSON result
 */
export async function queryClickHouse(config: IndexerConfig, sql: string): Promise<any[]> {
  const headers: Record<string, string> = { 
    'Content-Type': 'text/plain; charset=UTF-8' 
  }
  
  if (config.clickhouseAuth) {
    headers.Authorization = 'Basic ' + Buffer.from(config.clickhouseAuth).toString('base64')
  }
  
  const url = `${config.clickhouseUrl}/?database=${encodeURIComponent(config.clickhouseDb)}&default_format=JSONCompact`
  
  const res = await fetch(url, { method: 'POST', headers, body: sql })
  
  if (!res.ok) {
    const errorText = await res.text()
    throw new Error(`ClickHouse ${res.status}: ${errorText}`)
  }
  
  const result = await res.json()
  return result.data || []
}

/**
 * Executes a ClickHouse INSERT/DDL statement
 * @param config Indexer configuration
 * @param sql SQL statement to execute
 * @param options Optional configuration
 */
export async function executeClickHouse(
  config: IndexerConfig, 
  sql: string,
  options: { outputStats?: boolean } = {}
): Promise<void> {
  const headers: Record<string, string> = { 
    'Content-Type': 'text/plain; charset=UTF-8' 
  }
  
  if (config.clickhouseAuth) {
    headers.Authorization = 'Basic ' + Buffer.from(config.clickhouseAuth).toString('base64')
  }
  
  let url = `${config.clickhouseUrl}/?database=${encodeURIComponent(config.clickhouseDb)}`
  if (!options.outputStats) {
    url += '&output_format_write_statistics=0'
  }
  
  const res = await fetch(url, { method: 'POST', headers, body: sql })
  
  if (!res.ok) {
    const errorText = await res.text()
    throw new Error(`ClickHouse ${res.status}: ${errorText}`)
  }
}