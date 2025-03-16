#!/usr/bin/env python3
"""
Main entry point for the Pipeline Lock Manager CLI.
"""

import argparse
import sys
from gitlab_pipeline.lock_manager import GitLabPipelineLockManager
from config.settings import DEFAULT_TABLE_NAME, DEFAULT_REGION


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Pipeline Lock Manager Utility")
    parser.add_argument(
        "action", 
        choices=["acquire", "release"], 
        help="Action to perform"
    )
    parser.add_argument(
        "--table", 
        default=DEFAULT_TABLE_NAME, 
        help="DynamoDB table name"
    )
    parser.add_argument(
        "--region", 
        default=DEFAULT_REGION, 
        help="AWS region"
    )
    parser.add_argument(
        "--retry-interval", 
        type=int, 
        default=5, 
        help="Seconds to wait between retries"
    )
    parser.add_argument(
        "--max-retries", 
        type=int, 
        default=60, 
        help="Maximum number of retries"
    )
    parser.add_argument(
        "--force-after", 
        type=int, 
        default=None, 
        help="Force-acquire lock after this many seconds"
    )
    parser.add_argument(
        "--ttl", 
        type=int, 
        default=86400, 
        help="Lock time-to-live in seconds"
    )
    parser.add_argument(
        "--disable-ttl", 
        action="store_true", 
        help="Disable TTL for locks"
    )
    
    args = parser.parse_args()
    
    lock_manager = GitLabPipelineLockManager(
        dynamodb_table_name=args.table,
        dynamodb_region=args.region,
        enable_ttl=not args.disable_ttl,
        lock_ttl=args.ttl
    )
    
    if args.action == "acquire":
        success = lock_manager.acquire_lock(
            retry_interval=args.retry_interval,
            max_retries=args.max_retries,
            force_after=args.force_after
        )
        sys.exit(0 if success else 1)
        
    elif args.action == "release":
        success = lock_manager.release_lock()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 