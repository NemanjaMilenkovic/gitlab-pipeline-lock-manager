"""
Custom exceptions for the GitLab Pipeline Lock Manager.
"""

class PipelineLockError(Exception):
    """Base exception for all pipeline lock errors."""
    pass

class LockAcquisitionError(PipelineLockError):
    """Exception raised when a lock cannot be acquired."""
    pass

class LockReleaseError(PipelineLockError):
    """Exception raised when a lock cannot be released."""
    pass

class GitLabAPIError(PipelineLockError):
    """Exception raised when there is an error with the GitLab API."""
    pass

class EnvironmentVariableError(PipelineLockError):
    """Exception raised when a required environment variable is missing."""
    pass

class DynamoDBError(PipelineLockError):
    """Exception raised when there is an error with DynamoDB."""
    pass 