"""
Tests for the GitLab Pipeline Lock Manager.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from moto import mock_dynamodb

from gitlab_pipeline.lock_manager import GitLabPipelineLockManager, LockDetails
from gitlab_pipeline.exceptions import LockAcquisitionError, LockReleaseError


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing."""
    monkeypatch.setenv("CI_COMMIT_AUTHOR", "Test User")
    monkeypatch.setenv("CI_PROJECT_URL", "https://gitlab.com/test/project")
    monkeypatch.setenv("CI_PIPELINE_ID", "12345")
    monkeypatch.setenv("CI_JOB_ID", "67890")
    monkeypatch.setenv("CI_MERGE_REQUEST_PROJECT_URL", "https://gitlab.com/test/project")
    monkeypatch.setenv("CI_MERGE_REQUEST_IID", "42")
    monkeypatch.setenv("CI_COMMIT_SHA", "abcdef1234567890")


@pytest.fixture
def mock_dynamodb_table():
    """Set up a mock DynamoDB table for testing."""
    with mock_dynamodb():
        # Create the mock table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='test-locks',
            KeySchema=[
                {'AttributeName': 'LockID', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'LockID', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        
        yield table


@pytest.fixture
def lock_manager(mock_env_vars, mock_dynamodb_table):
    """Create a GitLabPipelineLockManager instance for testing."""
    manager = GitLabPipelineLockManager(
        dynamodb_table_name='test-locks',
        dynamodb_region='us-east-1',
        enable_ttl=True
    )
    
    # Mock the logger to prevent output during tests
    manager.logger = MagicMock()
    
    return manager


def test_acquire_lock_success(lock_manager):
    """Test successful lock acquisition."""
    # Mock the pipeline check
    with patch('gitlab_pipeline.lock_manager.get_pipeline', return_value={'status': 'running'}):
        assert lock_manager.acquire_lock() is True
        
        # Verify the lock was created
        response = lock_manager.table.get_item(Key={'LockID': 'PipelineLock'})
        assert 'Item' in response
        assert response['Item']['LockID'] == 'PipelineLock'
        assert 'ExpiresAt' in response['Item']  # TTL should be set


def test_acquire_lock_when_locked(lock_manager):
    """Test lock acquisition when a lock already exists."""
    # Create an existing lock
    lock_manager.table.put_item(
        Item={
            'LockID': 'PipelineLock',
            'CICommitAuthor': 'Another User',
            'Timestamp': datetime.now().isoformat(),
            'MergeRequestURL': 'https://gitlab.com/test/project/-/merge_requests/1',
            'PipelineURL': 'https://gitlab.com/test/project/-/pipelines/99999',
            'JobURL': 'https://gitlab.com/test/project/-/jobs/88888',
        }
    )
    
    # Mock the pipeline check
    with patch('gitlab_pipeline.lock_manager.get_pipeline', return_value={'status': 'running'}):
        # Set max_retries to 1 to avoid long test
        assert lock_manager.acquire_lock(max_retries=1) is False
        
        # Verify the original lock is still there
        response = lock_manager.table.get_item(Key={'LockID': 'PipelineLock'})
        assert response['Item']['PipelineURL'] == 'https://gitlab.com/test/project/-/pipelines/99999'


def test_force_acquire_stale_lock(lock_manager):
    """Test force-acquiring a stale lock."""
    # Create a stale lock (from 2 hours ago)
    stale_time = (datetime.now() - timedelta(hours=2)).isoformat()
    lock_manager.table.put_item(
        Item={
            'LockID': 'PipelineLock',
            'CICommitAuthor': 'Another User',
            'Timestamp': stale_time,
            'MergeRequestURL': 'https://gitlab.com/test/project/-/merge_requests/1',
            'PipelineURL': 'https://gitlab.com/test/project/-/pipelines/99999',
            'JobURL': 'https://gitlab.com/test/project/-/jobs/88888',
        }
    )
    
    # Mock the pipeline check
    with patch('gitlab_pipeline.lock_manager.get_pipeline', return_value={'status': 'running'}):
        # Force acquire after 1 hour
        assert lock_manager.acquire_lock(force_after=3600) is True
        
        # Verify our lock replaced the stale one
        response = lock_manager.table.get_item(Key={'LockID': 'PipelineLock'})
        assert response['Item']['CICommitAuthor'] == 'Test User'


def test_release_lock_success(lock_manager):
    """Test successful lock release."""
    # Create a lock for our pipeline
    lock_manager.table.put_item(
        Item={
            'LockID': 'PipelineLock',
            'CICommitAuthor': 'Test User',
            'Timestamp': datetime.now().isoformat(),
            'MergeRequestURL': 'https://gitlab.com/test/project/-/merge_requests/42',
            'PipelineURL': 'https://gitlab.com/test/project/-/pipelines/12345',
            'JobURL': 'https://gitlab.com/test/project/-/jobs/67890',
        }
    )
    
    assert lock_manager.release_lock() is True
    
    # Verify the lock was deleted
    response = lock_manager.table.get_item(Key={'LockID': 'PipelineLock'})
    assert 'Item' not in response


def test_release_nonexistent_lock(lock_manager):
    """Test releasing a lock that doesn't exist."""
    # No lock exists
    assert lock_manager.release_lock() is True
    
    # Verify no error was raised
    lock_manager.logger.warning.assert_called_once()


def test_release_someone_elses_lock(lock_manager):
    """Test attempting to release someone else's lock."""
    # Create a lock for another pipeline
    lock_manager.table.put_item(
        Item={
            'LockID': 'PipelineLock',
            'CICommitAuthor': 'Another User',
            'Timestamp': datetime.now().isoformat(),
            'MergeRequestURL': 'https://gitlab.com/test/project/-/merge_requests/1',
            'PipelineURL': 'https://gitlab.com/test/project/-/pipelines/99999',
            'JobURL': 'https://gitlab.com/test/project/-/jobs/88888',
        }
    )
    
    assert lock_manager.release_lock() is False
    
    # Verify the lock still exists
    response = lock_manager.table.get_item(Key={'LockID': 'PipelineLock'})
    assert 'Item' in response


def test_exit_if_pipeline_canceled(lock_manager):
    """Test exiting when pipeline is canceled."""
    with patch('gitlab_pipeline.lock_manager.get_pipeline', return_value={'status': 'canceled'}):
        with pytest.raises(SystemExit) as excinfo:
            lock_manager._exit_if_pipeline_canceled()
        
        assert excinfo.value.code == 0


def test_is_lock_stale(lock_manager):
    """Test stale lock detection."""
    # Create a lock from 2 hours ago
    stale_time = (datetime.now() - timedelta(hours=2)).isoformat()
    lock_details = LockDetails(
        author="Another User",
        timestamp=stale_time,
        merge_request_url="https://gitlab.com/test/project/-/merge_requests/1",
        pipeline_url="https://gitlab.com/test/project/-/pipelines/99999",
        job_url="https://gitlab.com/test/project/-/jobs/88888",
    )
    
    # Check if lock is stale after 1 hour
    assert lock_manager._is_lock_stale(lock_details, 3600) is True
    
    # Check if lock is stale after 3 hours (it's not)
    assert lock_manager._is_lock_stale(lock_details, 10800) is False


def test_is_our_lock(lock_manager):
    """Test lock ownership verification."""
    # Create a lock for our pipeline
    our_lock = LockDetails(
        author="Test User",
        timestamp=datetime.now().isoformat(),
        merge_request_url="https://gitlab.com/test/project/-/merge_requests/42",
        pipeline_url="https://gitlab.com/test/project/-/pipelines/12345",
        job_url="https://gitlab.com/test/project/-/jobs/67890",
    )
    
    # Create a lock for another pipeline
    other_lock = LockDetails(
        author="Another User",
        timestamp=datetime.now().isoformat(),
        merge_request_url="https://gitlab.com/test/project/-/merge_requests/1",
        pipeline_url="https://gitlab.com/test/project/-/pipelines/99999",
        job_url="https://gitlab.com/test/project/-/jobs/88888",
    )
    
    assert lock_manager._is_our_lock(our_lock) is True
    assert lock_manager._is_our_lock(other_lock) is False


def test_get_lock_details_nonexistent(lock_manager):
    """Test getting details for a nonexistent lock."""
    assert lock_manager._get_lock_details() is None


def test_get_lock_details_existing(lock_manager):
    """Test getting details for an existing lock."""
    # Create a lock
    timestamp = datetime.now().isoformat()
    lock_manager.table.put_item(
        Item={
            'LockID': 'PipelineLock',
            'CICommitAuthor': 'Test User',
            'Timestamp': timestamp,
            'MergeRequestURL': 'https://gitlab.com/test/project/-/merge_requests/42',
            'PipelineURL': 'https://gitlab.com/test/project/-/pipelines/12345',
            'JobURL': 'https://gitlab.com/test/project/-/jobs/67890',
            'ExpiresAt': int((datetime.now() + timedelta(days=1)).timestamp()),
        }
    )
    
    lock_details = lock_manager._get_lock_details()
    assert lock_details is not None
    assert lock_details.author == 'Test User'
    assert lock_details.timestamp == timestamp
    assert lock_details.merge_request_url == 'https://gitlab.com/test/project/-/merge_requests/42'
    assert lock_details.pipeline_url == 'https://gitlab.com/test/project/-/pipelines/12345'
    assert lock_details.job_url == 'https://gitlab.com/test/project/-/jobs/67890'
    assert lock_details.lock_ttl is not None