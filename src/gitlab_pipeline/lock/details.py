"""
Lock details model for the Pipeline Lock Manager.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class LockDetails:
    """Data class representing pipeline lock information."""
    author: str
    timestamp: str
    merge_request_url: str
    pipeline_url: str
    job_url: str
    lock_ttl: Optional[int] = None  # Time-to-live in seconds
    
    @classmethod
    def from_dynamodb_item(cls, item: dict) -> 'LockDetails':
        """
        Create a LockDetails instance from a DynamoDB item.
        
        Args:
            item: DynamoDB item
            
        Returns:
            LockDetails instance
        """
        return cls(
            author=item["CICommitAuthor"],
            timestamp=item["Timestamp"],
            merge_request_url=item["MergeRequestURL"],
            pipeline_url=item["PipelineURL"],
            job_url=item["JobURL"],
            lock_ttl=item.get("ExpiresAt")
        )
    
    def is_stale(self, stale_threshold: int) -> bool:
        """
        Check if the lock is stale based on its timestamp.
        
        Args:
            stale_threshold: Seconds after which a lock is considered stale
            
        Returns:
            True if lock is stale, False otherwise
        """
        try:
            lock_time = datetime.fromisoformat(self.timestamp)
            current_time = datetime.now()
            
            # Check if lock is older than the stale threshold
            return (current_time - lock_time).total_seconds() > stale_threshold
            
        except (ValueError, TypeError):
            return False
    
    def format_display(self) -> str:
        """
        Format the lock details for display.
        
        Returns:
            Formatted string
        """
        return f"""
------------
LOCK DETAILS
------------
AUTHOR:   {self.author}
TIME:     {self.timestamp}
MR:       {self.merge_request_url}
PIPELINE: {self.pipeline_url}
JOB:      {self.job_url}
""" 