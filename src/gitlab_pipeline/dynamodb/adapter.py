"""
DynamoDB adapter for the Pipeline Lock Manager.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from ..exceptions import DynamoDBError


class DynamoDBAdapter:
    """
    Adapter for interacting with DynamoDB for pipeline locks.
    
    This class handles all DynamoDB-specific operations, abstracting the
    database interactions from the lock manager.
    """
    
    def __init__(
        self,
        table_name: str,
        region_name: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the DynamoDB adapter.
        
        Args:
            table_name: Name of the DynamoDB table
            region_name: AWS region for DynamoDB
            logger: Logger instance
        """
        self.table_name = table_name
        self.region_name = region_name
        self.logger = logger or logging.getLogger(__name__)
        
        try:
            self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
            self.table = self.dynamodb.Table(table_name)
            self.logger.debug(f"Connected to DynamoDB table: {table_name}")
        except ClientError as e:
            error_msg = f"Failed to initialize DynamoDB client: {e}"
            self.logger.error(error_msg)
            raise DynamoDBError(error_msg)
    
    def get_item(self, lock_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item from DynamoDB by its lock ID.
        
        Args:
            lock_id: The lock ID to retrieve
            
        Returns:
            The item if found, None otherwise
            
        Raises:
            DynamoDBError: If there's an error with DynamoDB
        """
        try:
            response = self.table.get_item(Key={"LockID": lock_id})
            return response.get("Item")
        except ClientError as e:
            error_msg = f"Failed to get item from DynamoDB: {e}"
            self.logger.error(error_msg)
            raise DynamoDBError(error_msg)
    
    def put_item(self, item: Dict[str, Any], ttl_seconds: Optional[int] = None) -> bool:
        """
        Put an item in DynamoDB.
        
        Args:
            item: The item to put
            ttl_seconds: Time-to-live in seconds (if enabled)
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            DynamoDBError: If there's an error with DynamoDB
        """
        try:
            # Add TTL if specified
            if ttl_seconds is not None:
                expiry_time = int((datetime.now() + timedelta(seconds=ttl_seconds)).timestamp())
                item["ExpiresAt"] = expiry_time
            
            self.table.put_item(Item=item)
            return True
        except ClientError as e:
            error_msg = f"Failed to put item in DynamoDB: {e}"
            self.logger.error(error_msg)
            raise DynamoDBError(error_msg)
    
    def delete_item(self, lock_id: str) -> bool:
        """
        Delete an item from DynamoDB by its lock ID.
        
        Args:
            lock_id: The lock ID to delete
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            DynamoDBError: If there's an error with DynamoDB
        """
        try:
            self.table.delete_item(Key={"LockID": lock_id})
            return True
        except ClientError as e:
            error_msg = f"Failed to delete item from DynamoDB: {e}"
            self.logger.error(error_msg)
            raise DynamoDBError(error_msg)
    
    def query_by_lock_id(self, lock_id: str, limit: int = 2) -> Dict[str, Any]:
        """
        Query items by lock ID.
        
        Args:
            lock_id: The lock ID to query
            limit: Maximum number of items to return
            
        Returns:
            Query response
            
        Raises:
            DynamoDBError: If there's an error with DynamoDB
        """
        try:
            return self.table.query(
                KeyConditionExpression=Key("LockID").eq(lock_id),
                Limit=limit
            )
        except ClientError as e:
            error_msg = f"Failed to query DynamoDB: {e}"
            self.logger.error(error_msg)
            raise DynamoDBError(error_msg) 