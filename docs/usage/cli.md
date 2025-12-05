# CLI Reference

`ftm-lakehouse` provides a command-line interface for common operations.

## Global Options

```bash
ftm-lakehouse [OPTIONS] COMMAND [ARGS]
```

| Option | Description |
|--------|-------------|
| `--version` | Show version |
| `--settings` | Show current settings |
| `--uri` | Lakehouse URI (path) |
| `-d, --dataset` | Dataset name (required for most commands) |

## Dataset Commands

### List Datasets

```bash
# List dataset names
ftm-lakehouse ls

# Output to file
ftm-lakehouse ls -o datasets.txt
```

### Show Dataset Metadata

```bash
# Show all datasets with metadata
ftm-lakehouse datasets

# Output as JSON lines
ftm-lakehouse datasets -o datasets.jsonl
```

### Initialize/Update Dataset

```bash
# Create or update dataset metadata
ftm-lakehouse -d my_dataset make

# Also compute statistics
ftm-lakehouse -d my_dataset make --compute-stats

# Generate all exports
ftm-lakehouse -d my_dataset make --exports
```

## Entity Commands

### Write Entities

```bash
# Write entities from stdin
cat entities.ftm.json | ftm-lakehouse -d my_dataset write-entities

# Write from file
ftm-lakehouse -d my_dataset write-entities -i entities.ftm.json
```

### Stream Entities

```bash
# Stream entities to stdout
ftm-lakehouse -d my_dataset stream-entities

# Stream to file
ftm-lakehouse -d my_dataset stream-entities -o output.ftm.json
```

### Export Statements

```bash
# Export statement store to sorted CSV
ftm-lakehouse -d my_dataset export-statements
```

### Export Entities

```bash
# Export statements.csv to entities.ftm.json
ftm-lakehouse -d my_dataset export-entities
```

### Optimize Storage

```bash
# Optimize Delta Lake files
ftm-lakehouse -d my_dataset optimize

# Optimize and vacuum (remove old files)
ftm-lakehouse -d my_dataset optimize --vacuum
```

## Archive Commands

Archive commands are under the `archive` subcommand:

### List Files

```bash
# List all files with metadata
ftm-lakehouse -d my_dataset archive ls

# List only file paths
ftm-lakehouse -d my_dataset archive ls --keys

# List only checksums
ftm-lakehouse -d my_dataset archive ls --checksums

# Output to file
ftm-lakehouse -d my_dataset archive ls -o files.jsonl
```

### Get File Metadata

```bash
# Show file info
ftm-lakehouse -d my_dataset archive head <checksum>

# Output to file
ftm-lakehouse -d my_dataset archive head <checksum> -o file.json
```

### Retrieve File Content

```bash
# Write file to stdout
ftm-lakehouse -d my_dataset archive get <checksum>

# Write to file
ftm-lakehouse -d my_dataset archive get <checksum> -o document.pdf
```

## Mappings Commands

Mappings commands are under the `mappings` subcommand:

### List Mappings

```bash
# List all content hashes with mapping configs
ftm-lakehouse -d my_dataset mappings ls

# Output to file
ftm-lakehouse -d my_dataset mappings ls -o mappings.txt
```

### Get Mapping Config

```bash
# Show mapping configuration
ftm-lakehouse -d my_dataset mappings get <content_hash>

# Output to file
ftm-lakehouse -d my_dataset mappings get <content_hash> -o mapping.json
```

### Process Mappings

```bash
# Process a single mapping
ftm-lakehouse -d my_dataset mappings process <content_hash>

# Process all mappings in the dataset
ftm-lakehouse -d my_dataset mappings process
```

## Crawl Command

Crawl documents from local or remote sources:

```bash
# Crawl from local directory
ftm-lakehouse -d my_dataset crawl /path/to/documents

# Crawl from HTTP source
ftm-lakehouse -d my_dataset crawl https://example.com/files/

# With glob pattern
ftm-lakehouse -d my_dataset crawl /path --include "*.pdf"

# Exclude pattern
ftm-lakehouse -d my_dataset crawl /path --exclude "*.tmp"

# Don't skip existing files
ftm-lakehouse -d my_dataset crawl /path --no-skip-existing
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_URI` | Base path to lakehouse storage | `./data` |
| `LAKEHOUSE_JOURNAL_URI` | SQLAlchemy URI for journal database | `sqlite:///:memory:` |
| `LAKEHOUSE_LOG_LEVEL` | Logging level | `INFO` |
| `LAKEHOUSE_DEBUG` | Enable debug mode | `false` |

## Examples

### Complete Workflow

```bash
# Set up environment
export LAKEHOUSE_URI=./my_lakehouse

# Create a new dataset
ftm-lakehouse -d my_dataset make

# Crawl documents
ftm-lakehouse -d my_dataset crawl /path/to/documents

# Import entities
cat entities.ftm.json | ftm-lakehouse -d my_dataset write-entities

# Export everything
ftm-lakehouse -d my_dataset make --exports

# List what we have
ftm-lakehouse -d my_dataset archive ls --keys
ftm-lakehouse -d my_dataset stream-entities | head
```

### Working with S3

```bash
# Use S3 storage
export LAKEHOUSE_URI=s3://my-bucket/lakehouse
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# All commands work the same
ftm-lakehouse ls
ftm-lakehouse -d my_dataset make
```
