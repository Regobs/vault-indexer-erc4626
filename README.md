# ERC4626 Transaction Indexer

A blockchain indexer for ERC4626 vault transactions. Indexes deposits, withdrawals, and transfers to ClickHouse with automatic resume functionality and duplicate prevention.

## Quick Start

```bash
# Install dependencies
npm install

# Copy environment variables
cp .env.example .env

# Edit .env with your settings
# RPC_URL=https://your-rpc-endpoint
# CLICKHOUSE_URL=https://your-clickhouse-instance  
# VAULT=0x...

# Run indexer (auto-resumes from where it left off)
npm start
```

## Commands

```bash
# Smart resume (recommended)
npm start                    # Auto-detects where to continue

# Manual block ranges  
npm start 18000000           # From block to latest
npm start 18000000 18100000  # Specific range

# Development
npm run dev                  # Watch mode
```

## Features

- **Smart Resume**: Automatically continues from where you left off
- **Duplicate Prevention**: Won't create duplicate entries when resuming
- **Gap Filling**: Finds and processes missing block ranges
- **Error Recovery**: Handles RPC failures and network issues
- **Auto Schema**: Creates database tables automatically

## Configuration

Required environment variables:

```bash
RPC_URL=https://your-ethereum-rpc
CLICKHOUSE_URL=https://your-clickhouse-instance
VAULT=0x1234...  # ERC4626 vault contract address
```

Optional:
```bash
CLICKHOUSE_AUTH=user:password
CLICKHOUSE_DB=erc           # Default: erc
CHAIN=mainnet              # or sepolia  
CHUNK_BLOCKS=5000          # Blocks per batch
SLEEP_MS=900              # Delay between requests
```

## How It Works

1. **Fetches events** from your vault contract (deposits, withdrawals, transfers)
2. **Stores to ClickHouse** in optimized tables
3. **Tracks progress** in coverage table
4. **Resumes automatically** if interrupted
5. **Prevents duplicates** when reprocessing

## Resume Logic

- **Fresh start**: No previous data
- **Continue**: Picks up where you left off  
- **Fill gaps**: Processes missing/failed ranges first

The indexer is designed to be interrupted and restarted safely - it will never lose progress or create duplicates.

## Database Tables

Auto-created on first run:
- `erc4626_deposits` - Deposit events
- `erc4626_withdrawals` - Withdrawal events  
- `share_transfers` - Transfer events
- `indexer_coverage` - Progress tracking

## Performance

- Processes 5000 blocks per chunk by default
- Handles rate limits automatically
- Uses memory-efficient caching
- Parallel event processing within chunks
