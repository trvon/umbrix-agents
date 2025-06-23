"""
Shared utilities for agent coordination and orchestration.
Provides common patterns for task management, messaging, and inter-agent communication.
"""

import asyncio
import json
import time
import sys
import uuid
from typing import Dict, List, Optional, Any, Callable, Union
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from enum import Enum
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(Enum):
    """Task priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class AgentTask:
    """Represents a task for agent execution."""
    id: str
    agent_name: str
    task_type: str
    payload: Dict
    priority: TaskPriority = TaskPriority.NORMAL
    timeout: Optional[int] = None
    retry_count: int = 0
    max_retries: int = 3
    created_at: str = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    error: Optional[str] = None
    result: Optional[Dict] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc).isoformat()


@dataclass
class AgentStatus:
    """Represents the status of an agent."""
    agent_name: str
    is_active: bool
    last_heartbeat: str
    current_task: Optional[str] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    memory_usage: Optional[float] = None
    cpu_usage: Optional[float] = None
    metadata: Optional[Dict] = None


@dataclass
class CoordinationMessage:
    """Message for inter-agent communication."""
    id: str
    sender: str
    recipient: str
    message_type: str
    payload: Dict
    timestamp: str
    correlation_id: Optional[str] = None
    
    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


class TaskManager:
    """Manages task queues and execution for agent coordination."""
    
    def __init__(self, max_workers: int = 4):
        self.tasks: Dict[str, AgentTask] = {}
        self.task_queue: List[AgentTask] = []
        self.running_tasks: Dict[str, AgentTask] = {}
        self.completed_tasks: Dict[str, AgentTask] = {}
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.logger = logging.getLogger(__name__)
        
    def add_task(self, task: AgentTask) -> str:
        """Add a task to the queue."""
        self.tasks[task.id] = task
        self.task_queue.append(task)
        
        # Sort queue by priority
        self.task_queue.sort(key=lambda t: t.priority.value, reverse=True)
        
        self.logger.info(f"Added task {task.id} for agent {task.agent_name}")
        return task.id
    
    def get_next_task(self, agent_name: str = None) -> Optional[AgentTask]:
        """Get the next task from the queue, optionally filtered by agent."""
        for i, task in enumerate(self.task_queue):
            if agent_name is None or task.agent_name == agent_name:
                if task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.RUNNING
                    task.started_at = datetime.now(timezone.utc).isoformat()
                    self.running_tasks[task.id] = task
                    self.task_queue.pop(i)
                    return task
        return None
    
    def complete_task(self, task_id: str, result: Dict = None, error: str = None):
        """Mark a task as completed or failed."""
        if task_id in self.running_tasks:
            task = self.running_tasks.pop(task_id)
            task.completed_at = datetime.now(timezone.utc).isoformat()
            
            if error:
                task.status = TaskStatus.FAILED
                task.error = error
                
                # Check if we should retry
                if task.retry_count < task.max_retries:
                    task.retry_count += 1
                    task.status = TaskStatus.PENDING
                    task.started_at = None
                    task.error = None
                    self.task_queue.append(task)
                    self.task_queue.sort(key=lambda t: t.priority.value, reverse=True)
                    self.logger.info(f"Retrying task {task_id} (attempt {task.retry_count})")
                else:
                    self.completed_tasks[task_id] = task
                    self.logger.error(f"Task {task_id} failed after {task.retry_count} retries: {error}")
            else:
                task.status = TaskStatus.COMPLETED
                task.result = result
                self.completed_tasks[task_id] = task
                self.logger.info(f"Task {task_id} completed successfully")
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task."""
        # Cancel from queue
        for i, task in enumerate(self.task_queue):
            if task.id == task_id:
                task.status = TaskStatus.CANCELLED
                self.task_queue.pop(i)
                self.completed_tasks[task_id] = task
                return True
        
        # Cancel running task
        if task_id in self.running_tasks:
            task = self.running_tasks.pop(task_id)
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now(timezone.utc).isoformat()
            self.completed_tasks[task_id] = task
            return True
            
        return False
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get the status of a task."""
        if task_id in self.tasks:
            return self.tasks[task_id].status
        return None
    
    def get_queue_stats(self) -> Dict:
        """Get statistics about the task queue."""
        return {
            'pending': len([t for t in self.task_queue if t.status == TaskStatus.PENDING]),
            'running': len(self.running_tasks),
            'completed': len([t for t in self.completed_tasks.values() if t.status == TaskStatus.COMPLETED]),
            'failed': len([t for t in self.completed_tasks.values() if t.status == TaskStatus.FAILED]),
            'cancelled': len([t for t in self.completed_tasks.values() if t.status == TaskStatus.CANCELLED])
        }
    
    async def execute_task_async(self, task: AgentTask, task_function: Callable) -> Any:
        """Execute a task asynchronously with timeout handling."""
        try:
            if task.timeout:
                result = await asyncio.wait_for(
                    task_function(task.payload),
                    timeout=task.timeout
                )
            else:
                result = await task_function(task.payload)
                
            self.complete_task(task.id, result=result)
            return result
            
        except asyncio.TimeoutError:
            self.complete_task(task.id, error=f"Task timed out after {task.timeout} seconds")
            raise
        except Exception as e:
            self.complete_task(task.id, error=str(e))
            raise


class AgentRegistry:
    """Registry for tracking agent status and health."""
    
    def __init__(self, heartbeat_timeout: int = 300):
        self.agents: Dict[str, AgentStatus] = {}
        self.heartbeat_timeout = heartbeat_timeout
        self.logger = logging.getLogger(__name__)
    
    def register_agent(self, agent_name: str, metadata: Dict = None):
        """Register a new agent."""
        status = AgentStatus(
            agent_name=agent_name,
            is_active=True,
            last_heartbeat=datetime.now(timezone.utc).isoformat(),
            metadata=metadata or {}
        )
        self.agents[agent_name] = status
        self.logger.info(f"Registered agent: {agent_name}")
    
    def update_heartbeat(self, agent_name: str, task_id: str = None):
        """Update agent heartbeat."""
        if agent_name in self.agents:
            agent = self.agents[agent_name]
            agent.last_heartbeat = datetime.now(timezone.utc).isoformat()
            agent.is_active = True
            agent.current_task = task_id
    
    def mark_task_completed(self, agent_name: str, success: bool = True):
        """Mark a task as completed for an agent."""
        if agent_name in self.agents:
            agent = self.agents[agent_name]
            agent.current_task = None
            if success:
                agent.tasks_completed += 1
            else:
                agent.tasks_failed += 1
    
    def get_active_agents(self) -> List[str]:
        """Get list of active agent names."""
        now = datetime.now(timezone.utc)
        active_agents = []
        
        for agent_name, status in self.agents.items():
            last_heartbeat = datetime.fromisoformat(status.last_heartbeat.replace('Z', '+00:00'))
            if (now - last_heartbeat).total_seconds() < self.heartbeat_timeout:
                active_agents.append(agent_name)
            else:
                status.is_active = False
                
        return active_agents
    
    def get_agent_status(self, agent_name: str) -> Optional[AgentStatus]:
        """Get status of a specific agent."""
        return self.agents.get(agent_name)
    
    def get_registry_stats(self) -> Dict:
        """Get statistics about the agent registry."""
        active_agents = self.get_active_agents()
        total_completed = sum(agent.tasks_completed for agent in self.agents.values())
        total_failed = sum(agent.tasks_failed for agent in self.agents.values())
        
        return {
            'total_agents': len(self.agents),
            'active_agents': len(active_agents),
            'inactive_agents': len(self.agents) - len(active_agents),
            'total_tasks_completed': total_completed,
            'total_tasks_failed': total_failed
        }


class MessageBroker:
    """Simple message broker for inter-agent communication."""
    
    def __init__(self):
        self.message_queues: Dict[str, List[CoordinationMessage]] = {}
        self.subscribers: Dict[str, List[Callable]] = {}
        self.logger = logging.getLogger(__name__)
    
    def send_message(self, message: CoordinationMessage):
        """Send a message to an agent."""
        if message.recipient not in self.message_queues:
            self.message_queues[message.recipient] = []
            
        self.message_queues[message.recipient].append(message)
        
        # Notify subscribers
        if message.recipient in self.subscribers:
            for callback in self.subscribers[message.recipient]:
                try:
                    callback(message)
                except Exception as e:
                    self.logger.error(f"Error in message callback: {e}")
    
    def get_messages(self, agent_name: str, limit: int = 10) -> List[CoordinationMessage]:
        """Get messages for an agent."""
        if agent_name not in self.message_queues:
            return []
            
        messages = self.message_queues[agent_name][:limit]
        self.message_queues[agent_name] = self.message_queues[agent_name][limit:]
        
        return messages
    
    def subscribe(self, agent_name: str, callback: Callable):
        """Subscribe to messages for an agent."""
        if agent_name not in self.subscribers:
            self.subscribers[agent_name] = []
            
        self.subscribers[agent_name].append(callback)
    
    def broadcast_message(self, message_type: str, payload: Dict, sender: str):
        """Broadcast a message to all agents."""
        message_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()
        
        for agent_name in self.message_queues.keys():
            if agent_name != sender:  # Don't send to sender
                message = CoordinationMessage(
                    id=f"{message_id}_{agent_name}",
                    sender=sender,
                    recipient=agent_name,
                    message_type=message_type,
                    payload=payload,
                    timestamp=timestamp
                )
                self.send_message(message)


class CoordinationOrchestrator:
    """Main orchestrator for agent coordination."""
    
    def __init__(self, max_workers: int = 4):
        self.task_manager = TaskManager(max_workers)
        self.agent_registry = AgentRegistry()
        self.message_broker = MessageBroker()
        self.logger = logging.getLogger(__name__)
        self._running = False
    
    async def start(self):
        """Start the coordination orchestrator."""
        self._running = True
        self.logger.info("Coordination orchestrator started")
        
        # Start background tasks
        await asyncio.gather(
            self._health_check_loop(),
            self._cleanup_loop()
        )
    
    async def stop(self):
        """Stop the coordination orchestrator."""
        self._running = False
        self.task_manager.executor.shutdown(wait=True)
        self.logger.info("Coordination orchestrator stopped")
    
    def create_task(self, 
                   agent_name: str, 
                   task_type: str, 
                   payload: Dict,
                   priority: TaskPriority = TaskPriority.NORMAL,
                   timeout: int = None) -> str:
        """Create a new task."""
        task = AgentTask(
            id=str(uuid.uuid4()),
            agent_name=agent_name,
            task_type=task_type,
            payload=payload,
            priority=priority,
            timeout=timeout
        )
        
        return self.task_manager.add_task(task)
    
    def send_coordination_message(self, 
                                sender: str,
                                recipient: str,
                                message_type: str,
                                payload: Dict,
                                correlation_id: str = None) -> str:
        """Send a coordination message."""
        message = CoordinationMessage(
            id=str(uuid.uuid4()),
            sender=sender,
            recipient=recipient,
            message_type=message_type,
            payload=payload,
            timestamp="",  # Will be auto-generated in __post_init__
            correlation_id=correlation_id
        )
        
        self.message_broker.send_message(message)
        return message.id
    
    async def _health_check_loop(self):
        """Background loop for agent health checks."""
        while self._running:
            try:
                active_agents = self.agent_registry.get_active_agents()
                
                # Log inactive agents
                for agent_name, status in self.agent_registry.agents.items():
                    if not status.is_active:
                        self.logger.warning(f"Agent {agent_name} appears inactive")
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_loop(self):
        """Background loop for cleanup tasks."""
        while self._running:
            try:
                # Clean up old completed tasks
                cutoff_time = datetime.now(timezone.utc).timestamp() - 3600  # 1 hour ago
                
                to_remove = []
                for task_id, task in self.task_manager.completed_tasks.items():
                    if task.completed_at:
                        task_time = datetime.fromisoformat(task.completed_at.replace('Z', '+00:00')).timestamp()
                        if task_time < cutoff_time:
                            to_remove.append(task_id)
                
                for task_id in to_remove:
                    del self.task_manager.completed_tasks[task_id]
                    del self.task_manager.tasks[task_id]
                
                if to_remove:
                    self.logger.info(f"Cleaned up {len(to_remove)} old completed tasks")
                
                await asyncio.sleep(300)  # Clean up every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(300)
    
    def get_system_stats(self) -> Dict:
        """Get comprehensive system statistics."""
        return {
            'tasks': self.task_manager.get_queue_stats(),
            'agents': self.agent_registry.get_registry_stats(),
            'messages': {
                'queues': len(self.message_broker.message_queues),
                'total_messages': sum(len(queue) for queue in self.message_broker.message_queues.values())
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


class CircuitBreaker:
    """Circuit breaker pattern for fault tolerance."""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 expected_exception: type = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == 'OPEN':
            if self._should_attempt_reset():
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit breaker."""
        if self.last_failure_time is None:
            return False
        return (time.time() - self.last_failure_time) >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful execution."""
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def _on_failure(self):
        """Handle failed execution."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'


def create_task_payload(task_type: str, **kwargs) -> Dict:
    """Helper function to create standardized task payloads."""
    return {
        'task_type': task_type,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'parameters': kwargs
    }