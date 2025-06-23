"""
Test coverage for organizer_agent/coordination_tools.py

This module provides comprehensive test coverage for the agent coordination system,
including task management, agent registry, message brokering, orchestration, and circuit breaker patterns.
"""

import pytest
import asyncio
import time
import json
import uuid
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timezone, timedelta
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor

from organizer_agent.coordination_tools import (
    TaskStatus,
    TaskPriority,
    AgentTask,
    AgentStatus,
    CoordinationMessage,
    TaskManager,
    AgentRegistry,
    MessageBroker,
    CoordinationOrchestrator,
    CircuitBreaker,
    create_task_payload
)


class TestTaskStatus:
    """Test the TaskStatus enum."""
    
    def test_task_status_values(self):
        """Test TaskStatus enum values."""
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
        assert TaskStatus.CANCELLED.value == "cancelled"
        assert TaskStatus.TIMEOUT.value == "timeout"

    def test_task_status_enum_membership(self):
        """Test TaskStatus enum membership."""
        assert TaskStatus.PENDING in TaskStatus
        assert TaskStatus.RUNNING in TaskStatus
        assert TaskStatus.COMPLETED in TaskStatus
        assert TaskStatus.FAILED in TaskStatus
        assert TaskStatus.CANCELLED in TaskStatus
        assert TaskStatus.TIMEOUT in TaskStatus


class TestTaskPriority:
    """Test the TaskPriority enum."""
    
    def test_task_priority_values(self):
        """Test TaskPriority enum values."""
        assert TaskPriority.LOW.value == 1
        assert TaskPriority.NORMAL.value == 2
        assert TaskPriority.HIGH.value == 3
        assert TaskPriority.CRITICAL.value == 4

    def test_task_priority_ordering(self):
        """Test TaskPriority ordering for queue sorting."""
        assert TaskPriority.CRITICAL.value > TaskPriority.HIGH.value
        assert TaskPriority.HIGH.value > TaskPriority.NORMAL.value
        assert TaskPriority.NORMAL.value > TaskPriority.LOW.value


class TestAgentTask:
    """Test the AgentTask dataclass."""
    
    def test_agent_task_creation_minimal(self):
        """Test creating AgentTask with minimal required fields."""
        task = AgentTask(
            id="test-task-1",
            agent_name="test-agent",
            task_type="test-type",
            payload={"data": "test"}
        )
        
        assert task.id == "test-task-1"
        assert task.agent_name == "test-agent"
        assert task.task_type == "test-type"
        assert task.payload == {"data": "test"}
        assert task.priority == TaskPriority.NORMAL
        assert task.timeout is None
        assert task.retry_count == 0
        assert task.max_retries == 3
        assert task.created_at is not None
        assert task.started_at is None
        assert task.completed_at is None
        assert task.status == TaskStatus.PENDING
        assert task.error is None
        assert task.result is None

    def test_agent_task_creation_full(self):
        """Test creating AgentTask with all fields."""
        created_time = "2025-01-01T00:00:00+00:00"
        started_time = "2025-01-01T00:01:00+00:00"
        completed_time = "2025-01-01T00:02:00+00:00"
        
        task = AgentTask(
            id="test-task-2",
            agent_name="test-agent-2",
            task_type="complex-type",
            payload={"complex": "data", "parameters": [1, 2, 3]},
            priority=TaskPriority.HIGH,
            timeout=300,
            retry_count=1,
            max_retries=5,
            created_at=created_time,
            started_at=started_time,
            completed_at=completed_time,
            status=TaskStatus.RUNNING,
            error="test error",
            result={"output": "result"}
        )
        
        assert task.id == "test-task-2"
        assert task.agent_name == "test-agent-2"
        assert task.task_type == "complex-type"
        assert task.payload == {"complex": "data", "parameters": [1, 2, 3]}
        assert task.priority == TaskPriority.HIGH
        assert task.timeout == 300
        assert task.retry_count == 1
        assert task.max_retries == 5
        assert task.created_at == created_time
        assert task.started_at == started_time
        assert task.completed_at == completed_time
        assert task.status == TaskStatus.RUNNING
        assert task.error == "test error"
        assert task.result == {"output": "result"}

    def test_agent_task_post_init_auto_timestamp(self):
        """Test that created_at is auto-generated when not provided."""
        before_creation = datetime.now(timezone.utc)
        
        task = AgentTask(
            id="timestamp-test",
            agent_name="agent",
            task_type="type",
            payload={}
        )
        
        after_creation = datetime.now(timezone.utc)
        created_at = datetime.fromisoformat(task.created_at.replace('Z', '+00:00'))
        
        assert before_creation <= created_at <= after_creation

    def test_agent_task_post_init_preserve_existing_timestamp(self):
        """Test that existing created_at timestamp is preserved."""
        existing_timestamp = "2025-06-01T12:00:00+00:00"
        
        task = AgentTask(
            id="preserve-test",
            agent_name="agent",
            task_type="type",
            payload={},
            created_at=existing_timestamp
        )
        
        assert task.created_at == existing_timestamp


class TestAgentStatus:
    """Test the AgentStatus dataclass."""
    
    def test_agent_status_creation_minimal(self):
        """Test creating AgentStatus with minimal required fields."""
        status = AgentStatus(
            agent_name="test-agent",
            is_active=True,
            last_heartbeat="2025-01-01T00:00:00+00:00"
        )
        
        assert status.agent_name == "test-agent"
        assert status.is_active is True
        assert status.last_heartbeat == "2025-01-01T00:00:00+00:00"
        assert status.current_task is None
        assert status.tasks_completed == 0
        assert status.tasks_failed == 0
        assert status.memory_usage is None
        assert status.cpu_usage is None
        assert status.metadata is None

    def test_agent_status_creation_full(self):
        """Test creating AgentStatus with all fields."""
        metadata = {"version": "1.0", "capabilities": ["task1", "task2"]}
        
        status = AgentStatus(
            agent_name="full-agent",
            is_active=False,
            last_heartbeat="2025-01-01T00:00:00+00:00",
            current_task="task-123",
            tasks_completed=50,
            tasks_failed=5,
            memory_usage=512.5,
            cpu_usage=75.2,
            metadata=metadata
        )
        
        assert status.agent_name == "full-agent"
        assert status.is_active is False
        assert status.last_heartbeat == "2025-01-01T00:00:00+00:00"
        assert status.current_task == "task-123"
        assert status.tasks_completed == 50
        assert status.tasks_failed == 5
        assert status.memory_usage == 512.5
        assert status.cpu_usage == 75.2
        assert status.metadata == metadata


class TestCoordinationMessage:
    """Test the CoordinationMessage dataclass."""
    
    def test_coordination_message_creation_minimal(self):
        """Test creating CoordinationMessage with minimal required fields."""
        message = CoordinationMessage(
            id="",  # Will be auto-generated
            sender="sender-agent",
            recipient="recipient-agent",
            message_type="test-message",
            payload={"data": "test"},
            timestamp=""  # Will be auto-generated
        )
        
        assert message.sender == "sender-agent"
        assert message.recipient == "recipient-agent"
        assert message.message_type == "test-message"
        assert message.payload == {"data": "test"}
        assert message.correlation_id is None
        # Auto-generated fields
        assert message.id != ""
        assert message.timestamp != ""

    def test_coordination_message_creation_full(self):
        """Test creating CoordinationMessage with all fields."""
        message = CoordinationMessage(
            id="msg-123",
            sender="sender-agent",
            recipient="recipient-agent",
            message_type="full-message",
            payload={"complex": "data", "values": [1, 2, 3]},
            timestamp="2025-01-01T00:00:00+00:00",
            correlation_id="corr-456"
        )
        
        assert message.id == "msg-123"
        assert message.sender == "sender-agent"
        assert message.recipient == "recipient-agent"
        assert message.message_type == "full-message"
        assert message.payload == {"complex": "data", "values": [1, 2, 3]}
        assert message.timestamp == "2025-01-01T00:00:00+00:00"
        assert message.correlation_id == "corr-456"

    def test_coordination_message_post_init_auto_id(self):
        """Test that message ID is auto-generated when empty."""
        message = CoordinationMessage(
            id="",
            sender="sender",
            recipient="recipient",
            message_type="type",
            payload={},
            timestamp="2025-01-01T00:00:00+00:00"
        )
        
        assert message.id != ""
        assert len(message.id) == 36  # UUID length

    def test_coordination_message_post_init_auto_timestamp(self):
        """Test that timestamp is auto-generated when empty."""
        before_creation = datetime.now(timezone.utc)
        
        message = CoordinationMessage(
            id="test-id",
            sender="sender",
            recipient="recipient",
            message_type="type",
            payload={},
            timestamp=""
        )
        
        after_creation = datetime.now(timezone.utc)
        created_at = datetime.fromisoformat(message.timestamp.replace('Z', '+00:00'))
        
        assert before_creation <= created_at <= after_creation


class TestTaskManager:
    """Test the TaskManager class."""
    
    @pytest.fixture
    def task_manager(self):
        """Create a TaskManager instance for testing."""
        return TaskManager(max_workers=2)

    @pytest.fixture
    def sample_task(self):
        """Create a sample task for testing."""
        return AgentTask(
            id="sample-task",
            agent_name="sample-agent",
            task_type="sample-type",
            payload={"test": "data"}
        )

    def test_task_manager_initialization(self, task_manager):
        """Test TaskManager initialization."""
        assert task_manager.max_workers == 2
        assert len(task_manager.tasks) == 0
        assert len(task_manager.task_queue) == 0
        assert len(task_manager.running_tasks) == 0
        assert len(task_manager.completed_tasks) == 0
        assert isinstance(task_manager.executor, ThreadPoolExecutor)

    def test_add_task(self, task_manager, sample_task):
        """Test adding a task to the queue."""
        task_id = task_manager.add_task(sample_task)
        
        assert task_id == sample_task.id
        assert sample_task.id in task_manager.tasks
        assert sample_task in task_manager.task_queue
        assert len(task_manager.task_queue) == 1

    def test_add_multiple_tasks_priority_ordering(self, task_manager):
        """Test that tasks are ordered by priority in the queue."""
        low_task = AgentTask("low", "agent", "type", {}, priority=TaskPriority.LOW)
        high_task = AgentTask("high", "agent", "type", {}, priority=TaskPriority.HIGH)
        normal_task = AgentTask("normal", "agent", "type", {}, priority=TaskPriority.NORMAL)
        critical_task = AgentTask("critical", "agent", "type", {}, priority=TaskPriority.CRITICAL)
        
        # Add in random order
        task_manager.add_task(low_task)
        task_manager.add_task(high_task)
        task_manager.add_task(normal_task)
        task_manager.add_task(critical_task)
        
        # Should be ordered by priority (highest first)
        queue_priorities = [task.priority for task in task_manager.task_queue]
        expected_priorities = [TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]
        assert queue_priorities == expected_priorities

    def test_get_next_task_no_filter(self, task_manager):
        """Test getting next task without agent filter."""
        task1 = AgentTask("task1", "agent1", "type", {})
        task2 = AgentTask("task2", "agent2", "type", {})
        
        task_manager.add_task(task1)
        task_manager.add_task(task2)
        
        next_task = task_manager.get_next_task()
        
        assert next_task is not None
        assert next_task.id == "task1"  # First added
        assert next_task.status == TaskStatus.RUNNING
        assert next_task.started_at is not None
        assert next_task.id in task_manager.running_tasks
        assert len(task_manager.task_queue) == 1

    def test_get_next_task_with_agent_filter(self, task_manager):
        """Test getting next task with agent filter."""
        task1 = AgentTask("task1", "agent1", "type", {})
        task2 = AgentTask("task2", "agent2", "type", {})
        
        task_manager.add_task(task1)
        task_manager.add_task(task2)
        
        next_task = task_manager.get_next_task(agent_name="agent2")
        
        assert next_task is not None
        assert next_task.id == "task2"
        assert next_task.agent_name == "agent2"

    def test_get_next_task_empty_queue(self, task_manager):
        """Test getting next task from empty queue."""
        next_task = task_manager.get_next_task()
        assert next_task is None

    def test_get_next_task_no_matching_agent(self, task_manager):
        """Test getting next task with no matching agent."""
        task = AgentTask("task1", "agent1", "type", {})
        task_manager.add_task(task)
        
        next_task = task_manager.get_next_task(agent_name="agent2")
        assert next_task is None

    def test_complete_task_success(self, task_manager, sample_task):
        """Test completing a task successfully."""
        task_manager.add_task(sample_task)
        task_manager.get_next_task()  # Move to running
        
        result = {"output": "success"}
        task_manager.complete_task(sample_task.id, result=result)
        
        assert sample_task.id not in task_manager.running_tasks
        assert sample_task.id in task_manager.completed_tasks
        
        completed_task = task_manager.completed_tasks[sample_task.id]
        assert completed_task.status == TaskStatus.COMPLETED
        assert completed_task.result == result
        assert completed_task.completed_at is not None
        assert completed_task.error is None

    def test_complete_task_failure_no_retry(self, task_manager, sample_task):
        """Test completing a task with failure and no retries left."""
        sample_task.retry_count = 3  # Already at max retries
        task_manager.add_task(sample_task)
        task_manager.get_next_task()  # Move to running
        
        error_msg = "Task failed"
        task_manager.complete_task(sample_task.id, error=error_msg)
        
        assert sample_task.id not in task_manager.running_tasks
        assert sample_task.id in task_manager.completed_tasks
        
        completed_task = task_manager.completed_tasks[sample_task.id]
        assert completed_task.status == TaskStatus.FAILED
        assert completed_task.error == error_msg
        assert completed_task.result is None

    def test_complete_task_failure_with_retry(self, task_manager, sample_task):
        """Test completing a task with failure and retry available."""
        task_manager.add_task(sample_task)
        task_manager.get_next_task()  # Move to running
        
        error_msg = "Temporary failure"
        task_manager.complete_task(sample_task.id, error=error_msg)
        
        # Should be back in queue for retry
        assert sample_task.id not in task_manager.running_tasks
        assert sample_task.id not in task_manager.completed_tasks
        assert sample_task in task_manager.task_queue
        
        retry_task = task_manager.tasks[sample_task.id]
        assert retry_task.status == TaskStatus.PENDING
        assert retry_task.retry_count == 1
        assert retry_task.started_at is None
        assert retry_task.error is None

    def test_cancel_task_from_queue(self, task_manager, sample_task):
        """Test cancelling a task from the queue."""
        task_manager.add_task(sample_task)
        
        result = task_manager.cancel_task(sample_task.id)
        
        assert result is True
        assert sample_task not in task_manager.task_queue
        assert sample_task.id in task_manager.completed_tasks
        
        cancelled_task = task_manager.completed_tasks[sample_task.id]
        assert cancelled_task.status == TaskStatus.CANCELLED

    def test_cancel_running_task(self, task_manager, sample_task):
        """Test cancelling a running task."""
        task_manager.add_task(sample_task)
        task_manager.get_next_task()  # Move to running
        
        result = task_manager.cancel_task(sample_task.id)
        
        assert result is True
        assert sample_task.id not in task_manager.running_tasks
        assert sample_task.id in task_manager.completed_tasks
        
        cancelled_task = task_manager.completed_tasks[sample_task.id]
        assert cancelled_task.status == TaskStatus.CANCELLED
        assert cancelled_task.completed_at is not None

    def test_cancel_nonexistent_task(self, task_manager):
        """Test cancelling a nonexistent task."""
        result = task_manager.cancel_task("nonexistent")
        assert result is False

    def test_get_task_status(self, task_manager, sample_task):
        """Test getting task status."""
        task_manager.add_task(sample_task)
        
        status = task_manager.get_task_status(sample_task.id)
        assert status == TaskStatus.PENDING
        
        task_manager.get_next_task()  # Move to running
        status = task_manager.get_task_status(sample_task.id)
        assert status == TaskStatus.RUNNING

    def test_get_task_status_nonexistent(self, task_manager):
        """Test getting status of nonexistent task."""
        status = task_manager.get_task_status("nonexistent")
        assert status is None

    def test_get_queue_stats(self, task_manager):
        """Test getting queue statistics."""
        # Create tasks and move them through different states
        pending_task = AgentTask("pending", "agent", "type", {})
        running_task = AgentTask("running", "agent", "type", {})
        completed_task = AgentTask("completed", "agent", "type", {})
        failed_task = AgentTask("failed", "agent", "type", {})
        cancelled_task = AgentTask("cancelled", "agent", "type", {})
        
        # Add all tasks first
        task_manager.add_task(pending_task)
        task_manager.add_task(running_task)
        task_manager.add_task(completed_task)
        task_manager.add_task(failed_task)
        task_manager.add_task(cancelled_task)
        
        # Move one task to running state (keep it running)
        running_retrieved = task_manager.get_next_task()
        
        # Move another task to running then complete it successfully
        completed_retrieved = task_manager.get_next_task()
        task_manager.complete_task(completed_retrieved.id, result={"success": True})
        
        # Move another task to running then fail it (with max retries to prevent retry)
        failed_retrieved = task_manager.get_next_task()
        failed_retrieved.retry_count = 3  # Prevent retry
        task_manager.complete_task(failed_retrieved.id, error="Failed")
        
        # Cancel one task that's still in queue
        remaining_tasks = [t.id for t in task_manager.task_queue]
        if remaining_tasks:
            task_manager.cancel_task(remaining_tasks[0])
        
        stats = task_manager.get_queue_stats()
        
        # Check the actual state distribution
        assert stats['pending'] == 1  # One task still pending in queue
        assert stats['running'] == 1   # One task still running
        assert stats['completed'] == 1 # One task completed
        assert stats['failed'] == 1    # One task failed
        assert stats['cancelled'] == 1 # One task cancelled

    @pytest.mark.asyncio
    async def test_execute_task_async_success(self, task_manager):
        """Test executing a task asynchronously with success."""
        task = AgentTask("async-task", "agent", "type", {"input": "test"})
        task_manager.add_task(task)  # Add task to manager first
        retrieved_task = task_manager.get_next_task()  # Move to running state
        
        async def mock_task_function(payload):
            return {"output": "success", "input": payload["input"]}
        
        result = await task_manager.execute_task_async(retrieved_task, mock_task_function)
        
        assert result == {"output": "success", "input": "test"}
        assert retrieved_task.id in task_manager.completed_tasks
        completed_task = task_manager.completed_tasks[retrieved_task.id]
        assert completed_task.status == TaskStatus.COMPLETED
        assert completed_task.result == result

    @pytest.mark.asyncio
    async def test_execute_task_async_timeout(self, task_manager):
        """Test executing a task asynchronously with timeout."""
        task = AgentTask("timeout-task", "agent", "type", {}, timeout=1)
        task_manager.add_task(task)  # Add task to manager first
        retrieved_task = task_manager.get_next_task()  # Move to running state
        retrieved_task.retry_count = 3  # Prevent retry to ensure it fails permanently
        
        async def slow_task_function(payload):
            await asyncio.sleep(2)  # Longer than timeout
            return {"result": "too late"}
        
        with pytest.raises(asyncio.TimeoutError):
            await task_manager.execute_task_async(retrieved_task, slow_task_function)
        
        assert retrieved_task.id in task_manager.completed_tasks
        completed_task = task_manager.completed_tasks[retrieved_task.id]
        assert completed_task.status == TaskStatus.FAILED
        assert "timed out" in completed_task.error

    @pytest.mark.asyncio
    async def test_execute_task_async_exception(self, task_manager):
        """Test executing a task asynchronously with exception."""
        task = AgentTask("error-task", "agent", "type", {})
        task_manager.add_task(task)  # Add task to manager first
        retrieved_task = task_manager.get_next_task()  # Move to running state
        retrieved_task.retry_count = 3  # Prevent retry to ensure it fails permanently
        
        async def failing_task_function(payload):
            raise ValueError("Task failed")
        
        with pytest.raises(ValueError):
            await task_manager.execute_task_async(retrieved_task, failing_task_function)
        
        assert retrieved_task.id in task_manager.completed_tasks
        completed_task = task_manager.completed_tasks[retrieved_task.id]
        assert completed_task.status == TaskStatus.FAILED
        assert completed_task.error == "Task failed"


class TestAgentRegistry:
    """Test the AgentRegistry class."""
    
    @pytest.fixture
    def agent_registry(self):
        """Create an AgentRegistry instance for testing."""
        return AgentRegistry(heartbeat_timeout=60)

    def test_agent_registry_initialization(self, agent_registry):
        """Test AgentRegistry initialization."""
        assert agent_registry.heartbeat_timeout == 60
        assert len(agent_registry.agents) == 0

    def test_register_agent_minimal(self, agent_registry):
        """Test registering an agent with minimal parameters."""
        agent_registry.register_agent("test-agent")
        
        assert "test-agent" in agent_registry.agents
        agent_status = agent_registry.agents["test-agent"]
        
        assert agent_status.agent_name == "test-agent"
        assert agent_status.is_active is True
        assert agent_status.last_heartbeat is not None
        assert agent_status.metadata == {}

    def test_register_agent_with_metadata(self, agent_registry):
        """Test registering an agent with metadata."""
        metadata = {"version": "1.0", "capabilities": ["task1", "task2"]}
        agent_registry.register_agent("meta-agent", metadata=metadata)
        
        agent_status = agent_registry.agents["meta-agent"]
        assert agent_status.metadata == metadata

    def test_update_heartbeat(self, agent_registry):
        """Test updating agent heartbeat."""
        agent_registry.register_agent("heartbeat-agent")
        original_heartbeat = agent_registry.agents["heartbeat-agent"].last_heartbeat
        
        time.sleep(0.1)  # Small delay to ensure different timestamp
        agent_registry.update_heartbeat("heartbeat-agent", task_id="task-123")
        
        updated_agent = agent_registry.agents["heartbeat-agent"]
        assert updated_agent.last_heartbeat != original_heartbeat
        assert updated_agent.is_active is True
        assert updated_agent.current_task == "task-123"

    def test_update_heartbeat_nonexistent_agent(self, agent_registry):
        """Test updating heartbeat for nonexistent agent."""
        # Should not raise an error
        agent_registry.update_heartbeat("nonexistent-agent")
        assert "nonexistent-agent" not in agent_registry.agents

    def test_mark_task_completed_success(self, agent_registry):
        """Test marking task as completed successfully."""
        agent_registry.register_agent("task-agent")
        agent_registry.update_heartbeat("task-agent", task_id="task-123")
        
        agent_registry.mark_task_completed("task-agent", success=True)
        
        agent_status = agent_registry.agents["task-agent"]
        assert agent_status.current_task is None
        assert agent_status.tasks_completed == 1
        assert agent_status.tasks_failed == 0

    def test_mark_task_completed_failure(self, agent_registry):
        """Test marking task as completed with failure."""
        agent_registry.register_agent("task-agent")
        agent_registry.update_heartbeat("task-agent", task_id="task-456")
        
        agent_registry.mark_task_completed("task-agent", success=False)
        
        agent_status = agent_registry.agents["task-agent"]
        assert agent_status.current_task is None
        assert agent_status.tasks_completed == 0
        assert agent_status.tasks_failed == 1

    def test_mark_task_completed_nonexistent_agent(self, agent_registry):
        """Test marking task completed for nonexistent agent."""
        # Should not raise an error
        agent_registry.mark_task_completed("nonexistent-agent")

    def test_get_active_agents_all_active(self, agent_registry):
        """Test getting active agents when all are active."""
        agent_registry.register_agent("agent1")
        agent_registry.register_agent("agent2")
        agent_registry.register_agent("agent3")
        
        active_agents = agent_registry.get_active_agents()
        
        assert len(active_agents) == 3
        assert "agent1" in active_agents
        assert "agent2" in active_agents
        assert "agent3" in active_agents

    def test_get_active_agents_with_inactive(self, agent_registry):
        """Test getting active agents with some inactive."""
        # Create registry with short timeout for testing
        registry = AgentRegistry(heartbeat_timeout=1)
        
        registry.register_agent("active-agent")
        registry.register_agent("inactive-agent")
        
        # Make one agent inactive by setting old heartbeat
        old_time = datetime.now(timezone.utc) - timedelta(seconds=5)
        registry.agents["inactive-agent"].last_heartbeat = old_time.isoformat()
        
        active_agents = registry.get_active_agents()
        
        assert len(active_agents) == 1
        assert "active-agent" in active_agents
        assert "inactive-agent" not in active_agents
        
        # Check that inactive agent is marked as inactive
        assert registry.agents["inactive-agent"].is_active is False

    def test_get_agent_status_existing(self, agent_registry):
        """Test getting status of existing agent."""
        agent_registry.register_agent("status-agent")
        
        status = agent_registry.get_agent_status("status-agent")
        
        assert status is not None
        assert status.agent_name == "status-agent"
        assert status.is_active is True

    def test_get_agent_status_nonexistent(self, agent_registry):
        """Test getting status of nonexistent agent."""
        status = agent_registry.get_agent_status("nonexistent")
        assert status is None

    def test_get_registry_stats(self, agent_registry):
        """Test getting registry statistics."""
        # Register agents and simulate some activity
        agent_registry.register_agent("agent1")
        agent_registry.register_agent("agent2")
        agent_registry.register_agent("agent3")
        
        # Mark some tasks as completed/failed
        agent_registry.mark_task_completed("agent1", success=True)
        agent_registry.mark_task_completed("agent1", success=True)
        agent_registry.mark_task_completed("agent2", success=False)
        
        # Make one agent inactive
        old_time = datetime.now(timezone.utc) - timedelta(seconds=3600)
        agent_registry.agents["agent3"].last_heartbeat = old_time.isoformat()
        
        stats = agent_registry.get_registry_stats()
        
        assert stats['total_agents'] == 3
        assert stats['active_agents'] == 2  # agent1 and agent2
        assert stats['inactive_agents'] == 1  # agent3
        assert stats['total_tasks_completed'] == 2
        assert stats['total_tasks_failed'] == 1


class TestMessageBroker:
    """Test the MessageBroker class."""
    
    @pytest.fixture
    def message_broker(self):
        """Create a MessageBroker instance for testing."""
        return MessageBroker()

    @pytest.fixture
    def sample_message(self):
        """Create a sample coordination message."""
        return CoordinationMessage(
            id="msg-123",
            sender="sender-agent",
            recipient="recipient-agent",
            message_type="test-message",
            payload={"data": "test"},
            timestamp=datetime.now(timezone.utc).isoformat()
        )

    def test_message_broker_initialization(self, message_broker):
        """Test MessageBroker initialization."""
        assert len(message_broker.message_queues) == 0
        assert len(message_broker.subscribers) == 0

    def test_send_message_new_recipient(self, message_broker, sample_message):
        """Test sending message to new recipient."""
        message_broker.send_message(sample_message)
        
        assert "recipient-agent" in message_broker.message_queues
        assert len(message_broker.message_queues["recipient-agent"]) == 1
        assert message_broker.message_queues["recipient-agent"][0] == sample_message

    def test_send_message_existing_recipient(self, message_broker, sample_message):
        """Test sending message to existing recipient."""
        # Send first message
        message_broker.send_message(sample_message)
        
        # Send second message
        message2 = CoordinationMessage(
            id="msg-456",
            sender="sender2",
            recipient="recipient-agent",
            message_type="test2",
            payload={"data": "test2"},
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        message_broker.send_message(message2)
        
        assert len(message_broker.message_queues["recipient-agent"]) == 2

    def test_send_message_with_subscriber(self, message_broker, sample_message):
        """Test sending message with subscriber callback."""
        callback_called = False
        received_message = None
        
        def test_callback(message):
            nonlocal callback_called, received_message
            callback_called = True
            received_message = message
        
        message_broker.subscribe("recipient-agent", test_callback)
        message_broker.send_message(sample_message)
        
        assert callback_called is True
        assert received_message == sample_message

    def test_send_message_subscriber_exception(self, message_broker, sample_message):
        """Test sending message with subscriber that raises exception."""
        def failing_callback(message):
            raise ValueError("Callback failed")
        
        message_broker.subscribe("recipient-agent", failing_callback)
        
        # Should not raise exception, just log it
        message_broker.send_message(sample_message)
        
        # Message should still be queued
        assert len(message_broker.message_queues["recipient-agent"]) == 1

    def test_get_messages_existing_agent(self, message_broker):
        """Test getting messages for existing agent."""
        # Send multiple messages
        for i in range(5):
            message = CoordinationMessage(
                id=f"msg-{i}",
                sender="sender",
                recipient="agent",
                message_type="test",
                payload={"index": i},
                timestamp=datetime.now(timezone.utc).isoformat()
            )
            message_broker.send_message(message)
        
        # Get messages with limit
        messages = message_broker.get_messages("agent", limit=3)
        
        assert len(messages) == 3
        assert messages[0].payload["index"] == 0
        assert messages[1].payload["index"] == 1
        assert messages[2].payload["index"] == 2
        
        # Remaining messages should still be in queue
        assert len(message_broker.message_queues["agent"]) == 2

    def test_get_messages_nonexistent_agent(self, message_broker):
        """Test getting messages for nonexistent agent."""
        messages = message_broker.get_messages("nonexistent")
        assert messages == []

    def test_get_messages_empty_queue(self, message_broker):
        """Test getting messages from empty queue."""
        message_broker.message_queues["agent"] = []
        messages = message_broker.get_messages("agent")
        assert messages == []

    def test_subscribe_new_agent(self, message_broker):
        """Test subscribing to messages for new agent."""
        def callback(message):
            pass
        
        message_broker.subscribe("new-agent", callback)
        
        assert "new-agent" in message_broker.subscribers
        assert len(message_broker.subscribers["new-agent"]) == 1
        assert message_broker.subscribers["new-agent"][0] == callback

    def test_subscribe_existing_agent(self, message_broker):
        """Test subscribing multiple callbacks for existing agent."""
        def callback1(message):
            pass
        
        def callback2(message):
            pass
        
        message_broker.subscribe("agent", callback1)
        message_broker.subscribe("agent", callback2)
        
        assert len(message_broker.subscribers["agent"]) == 2

    def test_broadcast_message(self, message_broker):
        """Test broadcasting message to all agents."""
        # Create message queues for multiple agents
        message_broker.message_queues["agent1"] = []
        message_broker.message_queues["agent2"] = []
        message_broker.message_queues["agent3"] = []
        
        message_broker.broadcast_message(
            message_type="broadcast",
            payload={"announcement": "system update"},
            sender="system"
        )
        
        # All agents except sender should receive message
        assert len(message_broker.message_queues["agent1"]) == 1
        assert len(message_broker.message_queues["agent2"]) == 1
        assert len(message_broker.message_queues["agent3"]) == 1
        
        # Check message content
        message = message_broker.message_queues["agent1"][0]
        assert message.sender == "system"
        assert message.message_type == "broadcast"
        assert message.payload == {"announcement": "system update"}

    def test_broadcast_message_excludes_sender(self, message_broker):
        """Test that broadcast excludes the sender."""
        message_broker.message_queues["sender"] = []
        message_broker.message_queues["recipient"] = []
        
        message_broker.broadcast_message(
            message_type="broadcast",
            payload={"data": "test"},
            sender="sender"
        )
        
        # Sender should not receive their own broadcast
        assert len(message_broker.message_queues["sender"]) == 0
        assert len(message_broker.message_queues["recipient"]) == 1


class TestCoordinationOrchestrator:
    """Test the CoordinationOrchestrator class."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create a CoordinationOrchestrator instance for testing."""
        return CoordinationOrchestrator(max_workers=2)

    def test_orchestrator_initialization(self, orchestrator):
        """Test CoordinationOrchestrator initialization."""
        assert isinstance(orchestrator.task_manager, TaskManager)
        assert isinstance(orchestrator.agent_registry, AgentRegistry)
        assert isinstance(orchestrator.message_broker, MessageBroker)
        assert orchestrator._running is False

    def test_create_task(self, orchestrator):
        """Test creating a task through orchestrator."""
        task_id = orchestrator.create_task(
            agent_name="test-agent",
            task_type="test-task",
            payload={"data": "test"},
            priority=TaskPriority.HIGH,
            timeout=300
        )
        
        assert task_id is not None
        assert len(task_id) == 36  # UUID length
        
        # Check that task was added to task manager
        task = orchestrator.task_manager.tasks[task_id]
        assert task.agent_name == "test-agent"
        assert task.task_type == "test-task"
        assert task.payload == {"data": "test"}
        assert task.priority == TaskPriority.HIGH
        assert task.timeout == 300

    def test_send_coordination_message(self, orchestrator):
        """Test sending coordination message through orchestrator."""
        message_id = orchestrator.send_coordination_message(
            sender="sender-agent",
            recipient="recipient-agent",
            message_type="coordination",
            payload={"instruction": "start task"},
            correlation_id="corr-123"
        )
        
        assert message_id is not None
        assert len(message_id) == 36  # UUID length
        
        # Check that message was sent through message broker
        messages = orchestrator.message_broker.get_messages("recipient-agent")
        assert len(messages) == 1
        
        message = messages[0]
        assert message.sender == "sender-agent"
        assert message.recipient == "recipient-agent"
        assert message.message_type == "coordination"
        assert message.payload == {"instruction": "start task"}
        assert message.correlation_id == "corr-123"

    @pytest.mark.asyncio
    async def test_start_and_stop(self, orchestrator):
        """Test starting and stopping orchestrator."""
        # Mock the background loops to avoid running them
        async def mock_health_loop():
            pass
        
        async def mock_cleanup_loop():
            pass
        
        with patch.object(orchestrator, '_health_check_loop', mock_health_loop), \
             patch.object(orchestrator, '_cleanup_loop', mock_cleanup_loop):
            
            # Start orchestrator
            start_task = asyncio.create_task(orchestrator.start())
            
            # Give it a moment to start
            await asyncio.sleep(0.1)
            
            assert orchestrator._running is True
            
            # Stop orchestrator
            await orchestrator.stop()
            
            assert orchestrator._running is False
            
            # Cancel the start task
            start_task.cancel()
            try:
                await start_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_health_check_loop(self, orchestrator):
        """Test health check loop functionality."""
        orchestrator._running = True
        
        # Add some agents
        orchestrator.agent_registry.register_agent("active-agent")
        
        # Make one agent inactive
        old_time = datetime.now(timezone.utc) - timedelta(seconds=3600)
        orchestrator.agent_registry.agents["active-agent"].last_heartbeat = old_time.isoformat()
        
        # Run one iteration of health check
        with patch('asyncio.sleep') as mock_sleep:
            mock_sleep.side_effect = [None, asyncio.CancelledError()]  # Stop after one iteration
            
            try:
                await orchestrator._health_check_loop()
            except asyncio.CancelledError:
                pass
        
        # Should have been called once
        mock_sleep.assert_called_with(60)

    @pytest.mark.asyncio
    async def test_cleanup_loop(self, orchestrator):
        """Test cleanup loop functionality."""
        orchestrator._running = True
        
        # Add old completed task
        old_task = AgentTask("old-task", "agent", "type", {})
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        old_task.completed_at = old_time.isoformat()
        old_task.status = TaskStatus.COMPLETED
        
        orchestrator.task_manager.tasks[old_task.id] = old_task
        orchestrator.task_manager.completed_tasks[old_task.id] = old_task
        
        # Run one iteration of cleanup
        with patch('asyncio.sleep') as mock_sleep:
            mock_sleep.side_effect = [None, asyncio.CancelledError()]  # Stop after one iteration
            
            try:
                await orchestrator._cleanup_loop()
            except asyncio.CancelledError:
                pass
        
        # Old task should be removed
        assert old_task.id not in orchestrator.task_manager.completed_tasks
        assert old_task.id not in orchestrator.task_manager.tasks

    def test_get_system_stats(self, orchestrator):
        """Test getting system statistics."""
        # Add some data
        orchestrator.agent_registry.register_agent("agent1")
        orchestrator.create_task("agent1", "task", {"data": "test"})
        orchestrator.message_broker.send_message(CoordinationMessage(
            id="msg-1",
            sender="sender",
            recipient="agent1",
            message_type="test",
            payload={},
            timestamp=datetime.now(timezone.utc).isoformat()
        ))
        
        stats = orchestrator.get_system_stats()
        
        assert 'tasks' in stats
        assert 'agents' in stats
        assert 'messages' in stats
        assert 'timestamp' in stats
        
        assert stats['tasks']['pending'] == 1
        assert stats['agents']['total_agents'] == 1
        assert stats['messages']['queues'] == 1
        assert stats['messages']['total_messages'] == 1


class TestCircuitBreaker:
    """Test the CircuitBreaker class."""
    
    @pytest.fixture
    def circuit_breaker(self):
        """Create a CircuitBreaker instance for testing."""
        return CircuitBreaker(failure_threshold=3, recovery_timeout=5)

    def test_circuit_breaker_initialization(self, circuit_breaker):
        """Test CircuitBreaker initialization."""
        assert circuit_breaker.failure_threshold == 3
        assert circuit_breaker.recovery_timeout == 5
        assert circuit_breaker.expected_exception == Exception
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.last_failure_time is None
        assert circuit_breaker.state == 'CLOSED'

    def test_circuit_breaker_successful_call(self, circuit_breaker):
        """Test successful function call through circuit breaker."""
        def test_function(x, y):
            return x + y
        
        result = circuit_breaker.call(test_function, 2, 3)
        
        assert result == 5
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.state == 'CLOSED'

    def test_circuit_breaker_failure_within_threshold(self, circuit_breaker):
        """Test function failures within threshold."""
        def failing_function():
            raise ValueError("Test failure")
        
        # Call should fail but circuit should remain closed
        for i in range(2):  # Less than threshold
            with pytest.raises(ValueError):
                circuit_breaker.call(failing_function)
        
        assert circuit_breaker.failure_count == 2
        assert circuit_breaker.state == 'CLOSED'

    def test_circuit_breaker_failure_exceeds_threshold(self, circuit_breaker):
        """Test function failures exceeding threshold."""
        def failing_function():
            raise ValueError("Test failure")
        
        # Exceed failure threshold
        for i in range(3):
            with pytest.raises(ValueError):
                circuit_breaker.call(failing_function)
        
        assert circuit_breaker.failure_count == 3
        assert circuit_breaker.state == 'OPEN'

    def test_circuit_breaker_open_state_blocks_calls(self, circuit_breaker):
        """Test that open circuit breaker blocks calls."""
        def failing_function():
            raise ValueError("Test failure")
        
        # Trip the circuit breaker
        for i in range(3):
            with pytest.raises(ValueError):
                circuit_breaker.call(failing_function)
        
        # Now circuit is open, should block calls
        def successful_function():
            return "success"
        
        with pytest.raises(Exception, match="Circuit breaker is OPEN"):
            circuit_breaker.call(successful_function)

    def test_circuit_breaker_recovery_attempt(self, circuit_breaker):
        """Test circuit breaker recovery attempt."""
        def failing_function():
            raise ValueError("Test failure")
        
        # Trip the circuit breaker
        for i in range(3):
            with pytest.raises(ValueError):
                circuit_breaker.call(failing_function)
        
        # Wait for recovery timeout
        circuit_breaker.last_failure_time = time.time() - 10  # Simulate time passed
        
        def successful_function():
            return "recovered"
        
        # Should attempt recovery and succeed
        result = circuit_breaker.call(successful_function)
        
        assert result == "recovered"
        assert circuit_breaker.state == 'CLOSED'
        assert circuit_breaker.failure_count == 0

    def test_circuit_breaker_half_open_failure(self, circuit_breaker):
        """Test circuit breaker failure in half-open state."""
        def failing_function():
            raise ValueError("Test failure")
        
        # Trip the circuit breaker
        for i in range(3):
            with pytest.raises(ValueError):
                circuit_breaker.call(failing_function)
        
        # Simulate recovery timeout
        circuit_breaker.last_failure_time = time.time() - 10
        
        # First call after timeout should put it in half-open state
        # If it fails, should go back to open
        with pytest.raises(ValueError):
            circuit_breaker.call(failing_function)
        
        assert circuit_breaker.state == 'OPEN'
        assert circuit_breaker.failure_count == 4

    def test_circuit_breaker_custom_exception_type(self):
        """Test circuit breaker with custom expected exception type."""
        breaker = CircuitBreaker(failure_threshold=2, expected_exception=ValueError)
        
        def value_error_function():
            raise ValueError("Value error")
        
        def type_error_function():
            raise TypeError("Type error")
        
        # ValueError should be caught
        with pytest.raises(ValueError):
            breaker.call(value_error_function)
        
        assert breaker.failure_count == 1
        
        # TypeError should not be caught (should propagate and not count as failure)
        with pytest.raises(TypeError):
            breaker.call(type_error_function)
        
        assert breaker.failure_count == 1  # Should not increment

    def test_should_attempt_reset(self, circuit_breaker):
        """Test the _should_attempt_reset method."""
        # No previous failure (last_failure_time is None)
        assert circuit_breaker.last_failure_time is None
        assert circuit_breaker._should_attempt_reset() is False
        
        # Recent failure
        circuit_breaker.last_failure_time = time.time()
        assert circuit_breaker._should_attempt_reset() is False
        
        # Old failure
        circuit_breaker.last_failure_time = time.time() - 10
        assert circuit_breaker._should_attempt_reset() is True


class TestCreateTaskPayload:
    """Test the create_task_payload helper function."""
    
    def test_create_task_payload_basic(self):
        """Test creating basic task payload."""
        payload = create_task_payload("test-task")
        
        assert payload['task_type'] == "test-task"
        assert 'created_at' in payload
        assert 'parameters' in payload
        assert payload['parameters'] == {}

    def test_create_task_payload_with_parameters(self):
        """Test creating task payload with parameters."""
        payload = create_task_payload(
            "complex-task",
            param1="value1",
            param2=42,
            param3=["list", "data"]
        )
        
        assert payload['task_type'] == "complex-task"
        assert payload['parameters']['param1'] == "value1"
        assert payload['parameters']['param2'] == 42
        assert payload['parameters']['param3'] == ["list", "data"]

    def test_create_task_payload_timestamp_format(self):
        """Test that created_at timestamp is in ISO format."""
        payload = create_task_payload("timestamp-test")
        
        # Should be parseable as ISO format
        created_at = datetime.fromisoformat(payload['created_at'].replace('Z', '+00:00'))
        assert isinstance(created_at, datetime)


class TestIntegrationScenarios:
    """Integration tests combining multiple components."""
    
    @pytest.fixture
    def full_system(self):
        """Create a full coordination system for integration testing."""
        return CoordinationOrchestrator(max_workers=2)

    def test_complete_task_workflow(self, full_system):
        """Test complete task workflow from creation to completion."""
        # Register agent
        full_system.agent_registry.register_agent("worker-agent", metadata={"type": "worker"})
        
        # Create task
        task_id = full_system.create_task(
            agent_name="worker-agent",
            task_type="process-data",
            payload={"input": "test data"},
            priority=TaskPriority.HIGH
        )
        
        # Agent gets task
        task = full_system.task_manager.get_next_task("worker-agent")
        assert task is not None
        assert task.id == task_id
        assert task.status == TaskStatus.RUNNING
        
        # Update agent heartbeat
        full_system.agent_registry.update_heartbeat("worker-agent", task_id=task_id)
        
        # Complete task
        result = {"output": "processed data", "status": "success"}
        full_system.task_manager.complete_task(task_id, result=result)
        
        # Mark in agent registry
        full_system.agent_registry.mark_task_completed("worker-agent", success=True)
        
        # Verify final state
        assert task.status == TaskStatus.COMPLETED
        assert task.result == result
        
        agent_status = full_system.agent_registry.get_agent_status("worker-agent")
        assert agent_status.tasks_completed == 1
        assert agent_status.current_task is None

    def test_inter_agent_communication(self, full_system):
        """Test inter-agent communication through message broker."""
        # Register agents
        full_system.agent_registry.register_agent("coordinator")
        full_system.agent_registry.register_agent("worker1")
        full_system.agent_registry.register_agent("worker2")
        
        # Coordinator sends task assignment to worker1
        msg_id = full_system.send_coordination_message(
            sender="coordinator",
            recipient="worker1",
            message_type="task_assignment",
            payload={"task_id": "task-123", "priority": "high"},
            correlation_id="workflow-1"
        )
        
        # Worker1 gets message
        messages = full_system.message_broker.get_messages("worker1")
        assert len(messages) == 1
        
        message = messages[0]
        assert message.message_type == "task_assignment"
        assert message.payload["task_id"] == "task-123"
        
        # Worker1 sends status update back
        full_system.send_coordination_message(
            sender="worker1",
            recipient="coordinator",
            message_type="status_update",
            payload={"task_id": "task-123", "status": "in_progress"},
            correlation_id="workflow-1"
        )
        
        # Initialize message queues for workers (simulate agents being active)
        full_system.message_broker.message_queues["worker1"] = []
        full_system.message_broker.message_queues["worker2"] = []
        
        # Coordinator broadcasts system status
        full_system.message_broker.broadcast_message(
            message_type="system_status",
            payload={"system": "healthy", "load": "medium"},
            sender="coordinator"
        )
        
        # Both workers should receive broadcast
        worker1_messages = full_system.message_broker.get_messages("worker1")
        worker2_messages = full_system.message_broker.get_messages("worker2")
        
        assert len(worker1_messages) == 1  # Broadcast message
        assert len(worker2_messages) == 1  # Broadcast message
        assert worker1_messages[0].message_type == "system_status"

    def test_task_retry_mechanism(self, full_system):
        """Test task retry mechanism with failures."""
        # Create task
        task_id = full_system.create_task(
            agent_name="unreliable-agent",
            task_type="flaky-task",
            payload={"attempt": 1}
        )
        
        # First attempt fails
        task = full_system.task_manager.get_next_task()
        full_system.task_manager.complete_task(task_id, error="Network timeout")
        
        # Task should be back in queue for retry
        assert task.retry_count == 1
        assert task.status == TaskStatus.PENDING
        
        # Second attempt also fails
        task = full_system.task_manager.get_next_task()
        full_system.task_manager.complete_task(task_id, error="Service unavailable")
        
        # Third attempt also fails
        task = full_system.task_manager.get_next_task()
        full_system.task_manager.complete_task(task_id, error="Still failing")
        
        # Fourth attempt fails - should exceed max retries
        task = full_system.task_manager.get_next_task()
        full_system.task_manager.complete_task(task_id, error="Final failure")
        
        # Task should now be permanently failed
        final_task = full_system.task_manager.completed_tasks[task_id]
        assert final_task.status == TaskStatus.FAILED
        assert final_task.retry_count == 3
        assert "Final failure" in final_task.error

    def test_system_statistics_integration(self, full_system):
        """Test comprehensive system statistics."""
        # Set up complex system state
        full_system.agent_registry.register_agent("agent1")
        full_system.agent_registry.register_agent("agent2")
        
        # Create tasks with different priorities
        task1_id = full_system.create_task("agent1", "high-priority", {}, TaskPriority.HIGH)
        task2_id = full_system.create_task("agent2", "normal-priority", {}, TaskPriority.NORMAL)
        task3_id = full_system.create_task("agent1", "low-priority", {}, TaskPriority.LOW)
        
        # Process some tasks
        task1 = full_system.task_manager.get_next_task()  # Should be high priority
        full_system.task_manager.complete_task(task1.id, result={"success": True})
        full_system.agent_registry.mark_task_completed("agent1", success=True)
        
        task2 = full_system.task_manager.get_next_task()  # Should be normal priority  
        task2.retry_count = 3  # Prevent retry to ensure it goes to failed state
        full_system.task_manager.complete_task(task2.id, error="Failed")
        full_system.agent_registry.mark_task_completed("agent2", success=False)
        
        # Send some messages
        full_system.send_coordination_message("agent1", "agent2", "hello", {"msg": "hi"})
        full_system.send_coordination_message("agent2", "agent1", "reply", {"msg": "hello back"})
        
        # Get comprehensive stats
        stats = full_system.get_system_stats()
        
        # Verify task stats - task3 should still be pending
        assert stats['tasks']['pending'] == 1  # task3 still pending
        assert stats['tasks']['running'] == 0
        assert stats['tasks']['completed'] == 1  # task1
        assert stats['tasks']['failed'] == 1  # task2
        
        # Verify agent stats
        assert stats['agents']['total_agents'] == 2
        assert stats['agents']['active_agents'] == 2
        assert stats['agents']['total_tasks_completed'] == 1
        assert stats['agents']['total_tasks_failed'] == 1
        
        # Verify message stats
        assert stats['messages']['queues'] == 2
        assert stats['messages']['total_messages'] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])