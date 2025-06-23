"""
CrawlQueue: Priority-based queue for crawl task management.

Manages crawl tasks with priority queuing, Redis persistence, and task tracking.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum

import redis.asyncio as aioredis
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Task status enumeration."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


class CrawlTask(BaseModel):
    """Crawl task representation."""
    
    task_id: str = Field(default_factory=lambda: f"task_{int(time.time() * 1000)}")
    url: str
    priority: float = Field(ge=0.0, le=1.0)  # 0.0 = lowest, 1.0 = highest
    context: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: int = 300  # 5 minutes
    error_message: Optional[str] = None
    
    def is_expired(self) -> bool:
        """Check if task has expired."""
        if self.started_at and self.status == TaskStatus.IN_PROGRESS:
            elapsed = datetime.now(timezone.utc) - self.started_at
            return elapsed.total_seconds() > self.timeout_seconds
        return False
    
    def can_retry(self) -> bool:
        """Check if task can be retried."""
        return self.retry_count < self.max_retries


class QueueStats(BaseModel):
    """Queue statistics."""
    
    total_tasks: int = 0
    pending_tasks: int = 0
    in_progress_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    expired_tasks: int = 0
    avg_processing_time: float = 0.0
    throughput_per_minute: float = 0.0


class CrawlQueue:
    """
    Priority-based crawl task queue with Redis persistence.
    
    Features:
    - Priority-based task ordering
    - Redis-backed persistence
    - Task status tracking
    - Automatic retry mechanism
    - Task expiration handling
    - Queue statistics and monitoring
    """
    
    def __init__(self, redis_client: aioredis.Redis, max_size: int = 10000):
        """
        Initialize the CrawlQueue.
        
        Args:
            redis_client: Redis client for persistence
            max_size: Maximum queue size
        """
        self.redis = redis_client
        self.max_size = max_size
        self.logger = logging.getLogger(__name__)
        
        # Redis keys
        self.priority_queue_key = "crawl_queue:priority"
        self.tasks_key_prefix = "crawl_queue:task:"
        self.stats_key = "crawl_queue:stats"
        self.in_progress_key = "crawl_queue:in_progress"
        
        # Statistics
        self.stats = QueueStats()
        self._last_stats_update = time.time()
        
    async def add_task(self, task: CrawlTask) -> bool:
        """
        Add a task to the queue.
        
        Args:
            task: CrawlTask to add
            
        Returns:
            True if task was added successfully, False otherwise
        """
        try:
            # Check queue size limit
            queue_size = await self.size()
            if queue_size >= self.max_size:
                self.logger.warning(f"Queue at capacity ({self.max_size}), cannot add task")
                return False
            
            # Check if URL is already queued
            if await self._is_url_queued(task.url):
                self.logger.debug(f"URL already queued: {task.url}")
                return False
            
            task_key = f"{self.tasks_key_prefix}{task.task_id}"
            
            # Store task data
            await self.redis.set(task_key, task.model_dump_json())
            
            # Add to priority queue (higher priority = higher score)
            await self.redis.zadd(
                self.priority_queue_key,
                {task.task_id: task.priority}
            )
            
            # Update statistics
            self.stats.total_tasks += 1
            self.stats.pending_tasks += 1
            await self._update_stats()
            
            self.logger.info(f"Added task {task.task_id} for {task.url} (priority: {task.priority})")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding task to queue: {e}")
            return False
    
    async def get_next_task(self) -> Optional[CrawlTask]:
        """
        Get the next highest priority task from the queue.
        
        Returns:
            Next CrawlTask or None if queue is empty
        """
        try:
            # Get highest priority task (ZREVRANGE gets highest scores first)
            task_ids = await self.redis.zrevrange(
                self.priority_queue_key, 0, 0, withscores=False
            )
            
            if not task_ids:
                return None
            
            task_id = task_ids[0].decode('utf-8')
            task_key = f"{self.tasks_key_prefix}{task_id}"
            
            # Get task data
            task_data = await self.redis.get(task_key)
            if not task_data:
                # Task data missing, clean up
                await self.redis.zrem(self.priority_queue_key, task_id)
                return None
            
            # Parse task
            task = CrawlTask.model_validate_json(task_data)
            
            # Check if task has expired
            if task.is_expired():
                await self._mark_task_expired(task)
                return await self.get_next_task()  # Try next task
            
            # Mark task as in progress
            task.status = TaskStatus.IN_PROGRESS
            task.started_at = datetime.now(timezone.utc)
            
            # Update task in Redis
            await self.redis.set(task_key, task.model_dump_json())
            
            # Move from priority queue to in-progress set
            await self.redis.zrem(self.priority_queue_key, task_id)
            await self.redis.sadd(self.in_progress_key, task_id)
            
            # Update statistics
            self.stats.pending_tasks = max(0, self.stats.pending_tasks - 1)
            self.stats.in_progress_tasks += 1
            await self._update_stats()
            
            self.logger.info(f"Retrieved task {task_id} for {task.url}")
            return task
            
        except Exception as e:
            self.logger.error(f"Error getting next task: {e}")
            return None
    
    async def complete_task(self, task_id: str, success: bool = True, 
                           error_message: Optional[str] = None) -> bool:
        """
        Mark a task as completed or failed.
        
        Args:
            task_id: ID of task to complete
            success: Whether task completed successfully
            error_message: Error message if task failed
            
        Returns:
            True if task was marked successfully, False otherwise
        """
        try:
            task_key = f"{self.tasks_key_prefix}{task_id}"
            
            # Get current task
            task_data = await self.redis.get(task_key)
            if not task_data:
                self.logger.warning(f"Task not found: {task_id}")
                return False
            
            task = CrawlTask.model_validate_json(task_data)
            
            # Update task status
            task.completed_at = datetime.now(timezone.utc)
            task.error_message = error_message
            
            if success:
                task.status = TaskStatus.COMPLETED
                self.stats.completed_tasks += 1
                
                # Calculate processing time for statistics
                if task.started_at:
                    processing_time = (task.completed_at - task.started_at).total_seconds()
                    self._update_avg_processing_time(processing_time)
                
            else:
                # Check if task can be retried
                if task.can_retry():
                    task.retry_count += 1
                    task.status = TaskStatus.PENDING
                    task.started_at = None
                    
                    # Re-add to priority queue with slightly lower priority
                    retry_priority = max(0.0, task.priority - 0.05)
                    await self.redis.zadd(
                        self.priority_queue_key,
                        {task_id: retry_priority}
                    )
                    
                    # Update statistics for retry
                    self.stats.pending_tasks += 1
                    
                    self.logger.info(
                        f"Task {task_id} failed, retrying "
                        f"(attempt {task.retry_count}/{task.max_retries})"
                    )
                else:
                    task.status = TaskStatus.FAILED
                    self.stats.failed_tasks += 1
                    
                    self.logger.warning(
                        f"Task {task_id} failed permanently after "
                        f"{task.retry_count} retries: {error_message}"
                    )
            
            # Update task in Redis
            await self.redis.set(task_key, task.model_dump_json())
            
            # Remove from in-progress set
            await self.redis.srem(self.in_progress_key, task_id)
            
            # Update statistics
            self.stats.in_progress_tasks = max(0, self.stats.in_progress_tasks - 1)
            await self._update_stats()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error completing task {task_id}: {e}")
            return False
    
    async def get_task(self, task_id: str) -> Optional[CrawlTask]:
        """Get a specific task by ID."""
        try:
            task_key = f"{self.tasks_key_prefix}{task_id}"
            task_data = await self.redis.get(task_key)
            
            if task_data:
                return CrawlTask.model_validate_json(task_data)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting task {task_id}: {e}")
            return None
    
    async def size(self) -> int:
        """Get current queue size (pending + in progress)."""
        try:
            pending_count = await self.redis.zcard(self.priority_queue_key)
            in_progress_count = await self.redis.scard(self.in_progress_key)
            return pending_count + in_progress_count
        except Exception as e:
            self.logger.error(f"Error getting queue size: {e}")
            return 0
    
    async def is_empty(self) -> bool:
        """Check if queue is empty."""
        return await self.size() == 0
    
    async def cleanup_expired_tasks(self) -> int:
        """
        Clean up expired tasks and move them back to pending or mark as failed.
        
        Returns:
            Number of tasks cleaned up
        """
        cleaned_count = 0
        
        try:
            # Get all in-progress task IDs
            in_progress_ids = await self.redis.smembers(self.in_progress_key)
            
            for task_id_bytes in in_progress_ids:
                task_id = task_id_bytes.decode('utf-8')
                task = await self.get_task(task_id)
                
                if task and task.is_expired():
                    if task.can_retry():
                        # Move back to pending with lower priority
                        task.status = TaskStatus.PENDING
                        task.started_at = None
                        task.retry_count += 1
                        retry_priority = max(0.0, task.priority - 0.1)
                        
                        # Update task
                        task_key = f"{self.tasks_key_prefix}{task_id}"
                        await self.redis.set(task_key, task.model_dump_json())
                        
                        # Move back to priority queue
                        await self.redis.zadd(
                            self.priority_queue_key,
                            {task_id: retry_priority}
                        )
                        await self.redis.srem(self.in_progress_key, task_id)
                        
                        # Update statistics
                        self.stats.pending_tasks += 1
                        self.stats.in_progress_tasks -= 1
                        
                        self.logger.info(f"Moved expired task {task_id} back to pending")
                    else:
                        # Mark as expired/failed
                        await self._mark_task_expired(task)
                    
                    cleaned_count += 1
            
            if cleaned_count > 0:
                await self._update_stats()
                self.logger.info(f"Cleaned up {cleaned_count} expired tasks")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up expired tasks: {e}")
        
        return cleaned_count
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics."""
        try:
            # Update real-time counts
            pending_count = await self.redis.zcard(self.priority_queue_key)
            in_progress_count = await self.redis.scard(self.in_progress_key)
            
            self.stats.pending_tasks = pending_count
            self.stats.in_progress_tasks = in_progress_count
            
            # Calculate throughput
            current_time = time.time()
            time_diff = current_time - self._last_stats_update
            if time_diff > 0:
                completed_since_last = self.stats.completed_tasks
                self.stats.throughput_per_minute = (completed_since_last / time_diff) * 60
            
            return {
                "stats": self.stats.model_dump(),
                "queue_size": pending_count + in_progress_count,
                "capacity_used": (pending_count + in_progress_count) / self.max_size
            }
            
        except Exception as e:
            self.logger.error(f"Error getting queue stats: {e}")
            return {"error": str(e)}
    
    async def clear_completed_tasks(self, older_than_hours: int = 24) -> int:
        """
        Clear completed/failed tasks older than specified hours.
        
        Args:
            older_than_hours: Remove tasks completed more than this many hours ago
            
        Returns:
            Number of tasks removed
        """
        removed_count = 0
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        
        try:
            # Scan for task keys
            async for task_key in self.redis.scan_iter(match=f"{self.tasks_key_prefix}*"):
                try:
                    task_data = await self.redis.get(task_key)
                    if task_data:
                        task = CrawlTask.model_validate_json(task_data)
                        
                        # Remove if completed/failed and old enough
                        if (task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.EXPIRED] 
                            and task.completed_at 
                            and task.completed_at < cutoff_time):
                            
                            await self.redis.delete(task_key)
                            removed_count += 1
                            
                except Exception as e:
                    self.logger.warning(f"Error processing task for cleanup: {e}")
            
            if removed_count > 0:
                self.logger.info(f"Cleaned up {removed_count} old completed tasks")
            
        except Exception as e:
            self.logger.error(f"Error clearing completed tasks: {e}")
        
        return removed_count
    
    async def _is_url_queued(self, url: str) -> bool:
        """Check if a URL is already in the queue."""
        try:
            # This is a simple check - in production you might want to maintain a URL index
            async for task_key in self.redis.scan_iter(match=f"{self.tasks_key_prefix}*"):
                task_data = await self.redis.get(task_key)
                if task_data:
                    task = CrawlTask.model_validate_json(task_data)
                    if task.url == url and task.status in [TaskStatus.PENDING, TaskStatus.IN_PROGRESS]:
                        return True
            return False
        except Exception:
            return False
    
    async def _mark_task_expired(self, task: CrawlTask) -> None:
        """Mark a task as expired."""
        try:
            task.status = TaskStatus.EXPIRED
            task.completed_at = datetime.now(timezone.utc)
            
            task_key = f"{self.tasks_key_prefix}{task.task_id}"
            await self.redis.set(task_key, task.model_dump_json())
            
            # Remove from in-progress
            await self.redis.srem(self.in_progress_key, task.task_id)
            
            # Update statistics
            self.stats.expired_tasks += 1
            self.stats.in_progress_tasks = max(0, self.stats.in_progress_tasks - 1)
            
        except Exception as e:
            self.logger.error(f"Error marking task expired: {e}")
    
    def _update_avg_processing_time(self, processing_time: float) -> None:
        """Update average processing time."""
        if self.stats.completed_tasks <= 1:
            self.stats.avg_processing_time = processing_time
        else:
            # Rolling average
            total_tasks = self.stats.completed_tasks
            self.stats.avg_processing_time = (
                (self.stats.avg_processing_time * (total_tasks - 1) + processing_time) / total_tasks
            )
    
    async def _update_stats(self) -> None:
        """Update statistics in Redis."""
        try:
            await self.redis.set(self.stats_key, self.stats.model_dump_json())
            self._last_stats_update = time.time()
        except Exception as e:
            self.logger.warning(f"Error updating queue stats: {e}")