"""
GitLab Pipeline Lock Manager implementation.
"""

import logging
import time
from typing import Optional

from .dynamodb.adapter import DynamoDBAdapter
from .exceptions import LockAcquisitionError, LockReleaseError
from .lock.details import LockDetails
from .lock.operations import LockOperations
from .utils import get_logger


class GitLabPipelineLockManager:
    """
    Manages pipeline locks using DynamoDB to prevent concurrent execution of pipelines.
    
    This class provides functionality to acquire and release locks, with built-in
    retry mechanisms, stale lock detection, and detailed logging.
    """
    
    LOCK_ID = "PipelineLock"
    
    DEFAULT_RETRY_INTERVAL = 5  # seconds
    DEFAULT_MAX_RETRIES = 60    # 5 minutes with default interval
    
    DEFAULT_LOCK_TTL = 86400    # seconds
    
    def __init__(
        self, 
        dynamodb_table_name: str = "pipeline-locks",
        dynamodb_region: str = "eu-west-2",
        logger: Optional[logging.Logger] = None,
        enable_ttl: bool = True,
        lock_ttl: int = DEFAULT_LOCK_TTL
    ):
        """
        Initialize the Pipeline Lock Manager.
        
        Args:
            dynamodb_table_name: Name of the DynamoDB table for locks
            dynamodb_region: AWS region for DynamoDB
            logger: Custom logger (if None, one will be created)
            enable_ttl: Whether to enable TTL for locks
            lock_ttl: Time-to-live for locks in seconds
        """
        self.enable_ttl = enable_ttl
        self.lock_ttl = lock_ttl
        
        self.logger = logger or get_logger("PipelineLockManager")
        
        self.db_adapter = DynamoDBAdapter(
            table_name=dynamodb_table_name,
            region_name=dynamodb_region,
            logger=self.logger
        )
        
        self.lock_ops = LockOperations(
            db_adapter=self.db_adapter,
            lock_id=self.LOCK_ID,
            logger=self.logger
        )
    
    def acquire_lock(self, 
                     retry_interval: int = DEFAULT_RETRY_INTERVAL,
                     max_retries: int = DEFAULT_MAX_RETRIES,
                     force_after: Optional[int] = None) -> bool:
        """
        Acquire a pipeline lock, waiting if necessary.
        
        Args:
            retry_interval: Seconds to wait between retries
            max_retries: Maximum number of retries before giving up
            force_after: Seconds after which to force-acquire a stale lock
            
        Returns:
            bool: True if lock was acquired, False otherwise
        """
        retry_count = 0
        
        self.lock_ops.exit_if_pipeline_canceled()
        
        while retry_count < max_retries:
            current_lock = self.lock_ops.get_lock_details()
            
            if not current_lock:
                break  # No lock exists, proceed to acquire
            
            # Check if we should force-acquire a stale lock
            if force_after and current_lock.is_stale(force_after):
                self.logger.warning(f"Force-acquiring stale lock from {current_lock.author}")
                break
                
            self.logger.info(current_lock.format_display())
            
            self.logger.info(
                f"[WAITING] Lock unavailable. Retry {retry_count+1}/{max_retries} "
                f"in {retry_interval}s..."
            )
            
            time.sleep(retry_interval)
            retry_count += 1
            
            self.lock_ops.exit_if_pipeline_canceled()
        
        if retry_count >= max_retries:
            self.logger.error(f"[TIMEOUT] Failed to acquire lock after {max_retries} retries")
            return False
            
        # Acquire lock
        try:
            lock_item = self.lock_ops.create_lock_item()
            
            ttl = self.lock_ttl if self.enable_ttl else None
            
            self.db_adapter.put_item(lock_item, ttl)
            self.logger.info("[SUCCESS] Pipeline lock acquired successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to acquire lock: {e}")
            raise LockAcquisitionError(f"Failed to acquire lock: {e}")
    
    def release_lock(self) -> bool:
        """
        Release the pipeline lock.
        
        Returns:
            bool: True if lock was released, False otherwise
        """
        try:
            if not self.lock_ops.lock_exists():
                self.logger.warning(
                    "[WARNING] Lock doesn't exist. It may have expired or been manually deleted."
                )
                return True
            
            current_lock = self.lock_ops.get_lock_details()
            if current_lock and not self.lock_ops.is_our_lock(current_lock):
                self.logger.error(
                    f"[ERROR] Attempted to release someone else's lock: {current_lock.author}"
                )
                return False
                
            # Delete lock
            self.db_adapter.delete_item(self.LOCK_ID)
            
            self.logger.info("[SUCCESS] Pipeline lock released successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to release lock: {e}")
            raise LockReleaseError(f"Failed to release lock: {e}") 