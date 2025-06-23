#!/usr/bin/env python3
"""
Agent startup script that initializes and runs CTI agents based on configuration.
This script ensures agents are properly started in the correct order.
"""

import os
import sys
import yaml
import time
import signal
import threading
from pathlib import Path
from typing import Dict, List, Any
import logging

# Add the agents directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Import only the master coordinator - it manages all other agents
from organizer_agent.master_coordinator import CTIMasterCoordinatorAgent

# Setup logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_event = threading.Event()

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received shutdown signal, stopping agents...")
    shutdown_event.set()

def load_config() -> Dict[str, Any]:
    """Load configuration from config.yaml."""
    config_path = Path(__file__).parent / "config.yaml"
    if not config_path.exists():
        logger.error(f"Config file not found at {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Apply environment variables
    env_config = config.get('env', {})
    for key, value in env_config.items():
        if key not in os.environ:
            os.environ[key] = str(value)
    
    return config

def substitute_env_vars(value):
    """Recursively substitute environment variables in config values."""
    if isinstance(value, str):
        # Handle ${VAR:-default} pattern
        if value.startswith('${') and value.endswith('}'):
            var_expr = value[2:-1]
            if ':-' in var_expr:
                var_name, default_val = var_expr.split(':-', 1)
                return os.getenv(var_name, default_val)
            else:
                return os.getenv(var_expr, value)
        return value
    elif isinstance(value, dict):
        return {k: substitute_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [substitute_env_vars(v) for v in value]
    return value

def start_agent(agent_class, agent_config: Dict[str, Any], config: Dict[str, Any]):
    """Start the master coordinator agent."""
    name = agent_config.get('name', 'Unknown')
    enabled = agent_config.get('enabled', False)
    
    if not enabled:
        logger.info(f"Agent {name} is disabled, skipping")
        return None
    
    logger.info(f"Starting agent {name} in continuous_loop mode")
    
    try:
        # Get parameters and substitute environment variables recursively
        params = agent_config.get('params', {})
        params = substitute_env_vars(params)
        
        # Add kafka bootstrap servers if not present
        if 'bootstrap_servers' not in params and 'kafka_bootstrap_servers' not in params:
            kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            params['bootstrap_servers'] = kafka_servers
        
        # Create master coordinator instance
        agent = agent_class(**params)
        agent.name = name
        
        return agent
        
    except Exception as e:
        logger.error(f"Failed to start agent {name}: {e}")
        return None

def main():
    """Main startup function - now only starts the master coordinator."""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Ensure required Kafka topics exist
    logger.info("Ensuring required Kafka topics exist...")
    try:
        import subprocess
        result = subprocess.run([
            sys.executable, 
            str(Path(__file__).parent / "ensure_topics.py")
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            logger.error(f"Failed to create topics: {result.stderr}")
        else:
            logger.info("✓ Kafka topics verified")
    except Exception as e:
        logger.warning(f"Could not verify Kafka topics: {e}")
    
    # Load configuration
    config = load_config()
    
    # Start only the master coordinator - it will manage all other agents
    coordinator_configs = [c for c in config.get('orchestrators', []) 
                          if c.get('class') == 'CTIMasterCoordinatorAgent' and c.get('enabled')]
    
    if not coordinator_configs:
        logger.error("No enabled master coordinator found in configuration")
        sys.exit(1)
    
    coord_config = coordinator_configs[0]
    coordinator = start_agent(CTIMasterCoordinatorAgent, coord_config, config)
    
    if not coordinator:
        logger.error("Failed to create master coordinator")
        sys.exit(1)
    
    # Start master coordinator in main thread
    logger.info("Starting master coordinator (it will manage all sub-agents)")
    try:
        coordinator.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Master coordinator crashed: {e}")
    finally:
        # Cleanup
        if hasattr(coordinator, 'shutdown'):
            coordinator.shutdown()
        logger.info("Master coordinator stopped. Exiting.")

if __name__ == "__main__":
    main()