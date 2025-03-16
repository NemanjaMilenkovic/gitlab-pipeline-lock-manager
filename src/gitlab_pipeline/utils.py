"""
Utility functions for the GitLab Pipeline Lock Manager.
"""

import os
import logging
from typing import Optional

from .exceptions import EnvironmentVariableError

logger = logging.getLogger(__name__)

def get_env_var(var_name: str, default: Optional[str] = None) -> str:
    """
    Get an environment variable, with optional default value.
    
    Args:
        var_name: Name of the environment variable
        default: Default value if variable is not set
        
    Returns:
        Value of the environment variable
        
    Raises:
        EnvironmentVariableError: If variable is not set and no default is provided
    """
    value = os.environ.get(var_name, default)
    
    if value is None:
        error_message = f"Required environment variable {var_name} is not set"
        logger.error(error_message)
        raise EnvironmentVariableError(error_message)
        
    return value

def get_logger(name: str = "PipelineLockManager", level: int = logging.INFO) -> logging.Logger:
    """
    Get a configured logger.
    
    Args:
        name: Logger name
        level: Logging level
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Only add handler if not already added
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    
    return logger 