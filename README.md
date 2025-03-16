# Pipeline Lock Manager

DynamoDB-based locking system for GitLab CI/CD pipelines to prevent concurrent execution conflicts.

## Features

- Prevents concurrent pipeline execution
- Automatic stale lock detection and force-acquisition
- Time-to-live (TTL) for locks
- Detailed logging
- CLI utility for easy integration

## Requirements

- Python 3.7+
- boto3
- AWS credentials with DynamoDB access

## DynamoDB Table Setup

Create a table with:

- Name: `pipeline-locks` (configurable)
- Partition key: `LockID` (String)
- Enable TTL on `ExpiresAt` attribute

```bash
aws dynamodb create-table \
    --table-name pipeline-locks \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

aws dynamodb update-time-to-live \
    --table-name pipeline-locks \
    --time-to-live-specification "Enabled=true,AttributeName=ExpiresAt"
```

## Installation

```bash
pip install pipeline-lock-manager
```

## Usage

### In GitLab CI

```yaml
stages:
  - prepare
  - deploy

acquire_lock:
  stage: prepare
  script:
    - pipeline-lock acquire --table pipeline-locks

deploy:
  stage: deploy
  script:
    -  # Your deployment commands here
  after_script:
    - pipeline-lock release --table pipeline-locks
```

### Command Line

```bash
# Acquire a lock
pipeline-lock acquire --table pipeline-locks --force-after 3600

# Release a lock
pipeline-lock release --table pipeline-locks
```

### Python API

```python
from src.gitlab_pipeline.lock_manager import GitLabPipelineLockManager

lock = GitLabPipelineLockManager(dynamodb_table_name="pipeline-locks")
try:
    lock.acquire_lock(force_after=3600)
    # Your critical section here
finally:
    lock.release_lock()
```

## Advanced Options

```
pipeline-lock acquire --help
```

Options include:

- `--retry-interval`: Seconds to wait between retries
- `--max-retries`: Maximum number of retries
- `--force-after`: Force-acquire stale locks after this many seconds
- `--ttl`: Set custom TTL for locks
- `--disable-ttl`: Disable TTL for locks
