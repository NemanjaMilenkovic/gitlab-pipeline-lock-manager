"""
Lock operations for the Pipeline Lock Manager.
"""

import logging
import sys
import time
from datetime import datetime
from typing import Dict, Optional, Any

from ..dynamodb.adapter import DynamoDBAdapter
from ..exceptions import LockAcquisitionError, LockReleaseError
from ..gitlab_api import get_pipeline
from ..utils import get_env_var
from .details import LockDetails


class LockOperations:
    """
    Operations for managing pipeline locks.
    
    This class handles the logic for acquiring and releasing locks,
    abstracting the business logic from the lock manager.
    """
    
    def __init__(
        self,
        db_adapter: DynamoDBAdapter,
        lock_id: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the lock operations.
        
        Args:
            db_adapter: DynamoDB adapter
            lock_id: Lock ID to use
            logger: Logger instance
        """
        self.db_adapter = db_adapter
        self.lock_id = lock_id
        self.logger = logger or logging.getLogger(__name__)
    
    def get_lock_details(self) -> Optional[LockDetails]:
        """
        Get details about the current lock.
        
        Returns:
            LockDetails object if lock exists, None otherwise
        """
        item = self.db_adapter.get_item(self.lock_id)
        if not item:
            return None
        
        return LockDetails.from_dynamodb_item(item)
    
    def lock_exists(self) -> bool:
        """
        Check if a lock currently exists.
        
        Returns:
            True if lock exists, False otherwise
        """
        response = self.db_adapter.query_by_lock_id(self.lock_id)
        
        if response["Count"] > 1:
            self.logger.error(
                "[ERROR] Multiple lock items found with the same ID. Please investigate."
            )
            return True
            
        return response["Count"] == 1
    
    def is_our_lock(self, lock_details: LockDetails) -> bool:
        """
        Check if the current lock belongs to this pipeline.
        
        Args:
            lock_details: Lock details to check
            
        Returns:
            True if lock belongs to this pipeline, False otherwise
        """
        try:
            pipeline_url = self._get_pipeline_url()
            return lock_details.pipeline_url == pipeline_url
        except Exception:
            return False
    
    def create_lock_item(self) -> Dict[str, Any]:
        """
        Create a lock item for DynamoDB.
        
        Returns:
            Lock item dictionary
        """
        return {
            "LockID": self.lock_id,
            "MergeRequestURL": self._get_merge_request_url(),
            "CICommitAuthor": get_env_var("CI_COMMIT_AUTHOR"),
            "Timestamp": datetime.now().isoformat(),
            "PipelineURL": self._get_pipeline_url(),
            "JobURL": self._get_job_url(),
        }
    
    def exit_if_pipeline_canceled(self) -> None:
        """
        Check if the current pipeline is canceled and exit if it is.
        """
        try:
            pipeline_id = get_env_var("CI_PIPELINE_ID")
            pipeline_status = get_pipeline(pipeline_id)["status"]
            
            if pipeline_status == "canceled":
                self.logger.info("[CANCELED] Pipeline was canceled, exiting...")
                sys.exit(0)
                
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to check pipeline status: {e}")
    
    def _get_merge_request_url(self) -> str:
        """Get the merge request URL for the current pipeline."""
        try:
            project_url = get_env_var('CI_MERGE_REQUEST_PROJECT_URL')
            mr_iid = get_env_var('CI_MERGE_REQUEST_IID')
            return f"{project_url}/-/merge_requests/{mr_iid}"
        except Exception:
            # Handle case where pipeline is not from a merge request
            return f"{get_env_var('CI_PROJECT_URL')}/-/commit/{get_env_var('CI_COMMIT_SHA')}"
    
    def _get_pipeline_url(self) -> str:
        """Get the pipeline URL for the current pipeline."""
        return f"{get_env_var('CI_PROJECT_URL')}/-/pipelines/{get_env_var('CI_PIPELINE_ID')}"
    
    def _get_job_url(self) -> str:
        """Get the job URL for the current job."""
        return f"{get_env_var('CI_PROJECT_URL')}/-/jobs/{get_env_var('CI_JOB_ID')}" 