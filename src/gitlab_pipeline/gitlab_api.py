"""
GitLab API interaction functions.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
import urllib.request
import urllib.error

from .utils import get_env_var
from .exceptions import GitLabAPIError

logger = logging.getLogger(__name__)

def get_pipeline(pipeline_id: str) -> Dict[str, Any]:
    """
    Get pipeline information from GitLab API.
    
    Args:
        pipeline_id: The ID of the pipeline to get
        
    Returns:
        Dict containing pipeline information
        
    Raises:
        GitLabAPIError: If the API request fails
    """
    try:
        gitlab_token = get_env_var("CI_JOB_TOKEN")
        project_id = get_env_var("CI_PROJECT_ID")
        gitlab_api_url = get_env_var("CI_API_V4_URL", "https://gitlab.com/api/v4")
        
        url = f"{gitlab_api_url}/projects/{project_id}/pipelines/{pipeline_id}"
        
        headers = {
            "PRIVATE-TOKEN": gitlab_token,
            "Content-Type": "application/json"
        }
        
        request = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode())
            
    except urllib.error.HTTPError as e:
        error_message = f"GitLab API error: {e.code} - {e.reason}"
        logger.error(error_message)
        
        if e.code == 404:
            raise GitLabAPIError(f"Pipeline {pipeline_id} not found")
        else:
            raise GitLabAPIError(error_message)
            
    except Exception as e:
        error_message = f"Failed to get pipeline information: {e}"
        logger.error(error_message)
        raise GitLabAPIError(error_message) 