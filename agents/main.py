import yaml
from pathlib import Path

import pkgutil
import importlib
from google.adk.agents import Agent
import os
import logging
import inspect
import json
import sys
import six  # ensure kafka.vendor.six import resolution
sys.modules.setdefault('kafka.vendor.six', six)
sys.modules.setdefault('kafka.vendor.six.moves', six.moves)

logging.basicConfig(level=os.getenv("AGENT_LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

def load_agents(config):
    agents = []
    base_path = Path(__file__).parent
    agent_pkgs = [p.name for p in base_path.iterdir() if p.is_dir() and p.name.endswith('_agent')]
    for pkg in agent_pkgs:
        pkg_path = base_path / pkg
        for _, module_name, _ in pkgutil.iter_modules([str(pkg_path)]):
            module = importlib.import_module(f"{pkg}.{module_name}")
            for attr in dir(module):
                cls = getattr(module, attr)
                if isinstance(cls, type) and issubclass(cls, Agent) and cls is not Agent:
                    key_base = module_name.replace('_agent', '')
                    key = key_base + ('_feeds' if key_base in ['misp_feed', 'rss_collector'] else 'servers')
                    param = 'feeds' if 'misp' in module_name or 'rss' in module_name else 'servers'
                    arg = config.get(key, [])
                    # Dynamically pass a name so agent instances have a .name attribute
                    kwargs = {param: arg, "name": module_name}
                    instance = cls(**kwargs)
                    # Ensure dynamically loaded agents have a default execution_mode
                    if not hasattr(instance, 'execution_mode'):
                        instance.execution_mode = 'run_once'
                    agents.append(instance)
    return agents

# Helper to substitute environment variables in config values
def _substitute_env_vars(value):
    if isinstance(value, str):
        return os.path.expandvars(value)
    elif isinstance(value, list):
        return [_substitute_env_vars(v) for v in value]
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    return value

# Load agents based on config.yaml entries
def load_agents_from_config(agent_configs: list) -> dict:
    loaded_agents = {"runnable": [], "tool_only": [], "coordinators": []}
    for conf in agent_configs:
        if not conf.get("enabled", False):
            logger.info(f"Skipping disabled agent {conf.get('name')}")
            continue
        module_str = conf["module"]
        class_str = conf["class"]
        agent_name = conf.get("name", class_str)
        execution_mode = conf.get("execution_mode", "run_once")
        params = _substitute_env_vars(conf.get("params", {}))
        try:
            module = importlib.import_module(module_str)
            agent_class = getattr(module, class_str)

            # Build constructor kwargs safely
            constructor_kwargs = {}
            if isinstance(params, dict):
                constructor_kwargs.update(params)
            constructor_kwargs["name"] = agent_name
            if "execution_mode" in inspect.signature(agent_class.__init__).parameters:
                constructor_kwargs["execution_mode"] = execution_mode

            instance = agent_class(**constructor_kwargs)
            # Ensure every agent instance has an execution_mode attribute
            if not hasattr(instance, 'execution_mode'):
                instance.execution_mode = execution_mode
            # Ensure every agent instance has a name attribute
            if not hasattr(instance, "name"):
                instance.name = agent_name

            if "execution_mode" not in constructor_kwargs and (
                hasattr(agent_class, 'Config')
                and (not hasattr(agent_class.Config, 'extra') or agent_class.Config.extra != 'forbid')
            ):
                try:
                    instance.execution_mode = execution_mode
                except AttributeError:
                    logger.warning(f"Could not set execution_mode directly on {agent_name}. It might need to be an __init__ param or handled internally.")

            effective_mode = constructor_kwargs.get("execution_mode") or getattr(instance, "execution_mode", "unknown")

            if effective_mode == "tool_only":
                loaded_agents["tool_only"].append(instance)
                logger.info(f"Loaded TOOL agent: {agent_name}")
            elif effective_mode == "coordinator":
                loaded_agents["coordinators"].append(instance)
                logger.info(f"Loaded COORDINATOR agent: {agent_name}")
            else:
                loaded_agents["runnable"].append(instance)
                logger.info(f"Loaded RUNNABLE agent: {agent_name} (mode={effective_mode})")
        except Exception as e:
            logger.error(f"Failed loading agent {agent_name}: {e}", exc_info=True)
    return loaded_agents

# Load orchestrator agents based on config.yaml entries
def load_orchestrators(orchestrator_configs: list, loaded_agents: dict) -> list:
    orchestrators = []
    for conf in orchestrator_configs:
        if not conf.get("enabled", False):
            logger.info(f"Skipping disabled orchestrator {conf.get('name')}")
            continue
        module_str = conf["module"]
        class_str = conf["class"]
        orchestrator_name = conf.get("name", class_str)
        execution_mode = conf.get("execution_mode", "run_once_pipeline")
        params = _substitute_env_vars(conf.get("params", {}))
        sub_names = conf.get("sub_agent_names", [])
        # Resolve sub-agents by name
        candidates = (loaded_agents.get("runnable", []) +
                      loaded_agents.get("tool_only", []) +
                      loaded_agents.get("coordinators", []))
        sub_agents = [a for a in candidates if a.name in sub_names]
        # Resolve tool-only agents if specified
        tool_names = conf.get("tool_agent_names", [])
        tools = [t for t in loaded_agents.get("tool_only", []) if t.name in tool_names]
        try:
            module = importlib.import_module(module_str)
            orchestrator_class = getattr(module, class_str)
            # Instantiate with sub_agents and optional tools
            # Pass execution_mode in constructor if it's a known param for the class
            # Build constructor kwargs safely
            constructor_kwargs = {}
            if isinstance(params, dict):
                constructor_kwargs.update(params)
            constructor_kwargs["name"] = orchestrator_name
            constructor_kwargs["sub_agents"] = sub_agents
            if "execution_mode" in inspect.signature(orchestrator_class.__init__).parameters:
                 constructor_kwargs["execution_mode"] = execution_mode

            if tools:
                instance = orchestrator_class(**constructor_kwargs, tools=tools)
            else:
                instance = orchestrator_class(**constructor_kwargs)
            # Ensure every orchestrator instance has a name attribute
            if not hasattr(instance, "name"):
                instance.name = orchestrator_name

            # Only set execution_mode if class provides Config with extra not 'forbid'
            if "execution_mode" not in constructor_kwargs and (
                hasattr(orchestrator_class, 'Config')
                and (not hasattr(orchestrator_class.Config, 'extra') or orchestrator_class.Config.extra != 'forbid')
            ):
                try:
                    instance.execution_mode = execution_mode
                except AttributeError: # Or specific Pydantic error if possible
                    logger.warning(f"Could not set execution_mode directly on {orchestrator_name}. It might need to be an __init__ param or handled internally.")

            orchestrators.append(instance)
            logger.info(f"Loaded ORCHESTRATOR agent: {orchestrator_name} (mode={execution_mode if 'execution_mode' in constructor_kwargs else 'unknown; check class'})")
        except Exception as e:
            logger.error(f"Failed loading orchestrator {orchestrator_name}: {e}", exc_info=True)
    return orchestrators

# Replace main execution logic
def main():
    # Load application config
    config_path = Path(__file__).parent / "config.yaml"
    if not config_path.exists():
        logger.error(f"Main config file not found at {config_path}. Exiting.")
        return
    app_config = yaml.safe_load(config_path.read_text()) or {}
    # If running locally outside Docker, default Kafka to localhost
    if os.getenv("KAFKA_BOOTSTRAP_SERVERS") is None:
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"

    # Load environment variables from config
    env_config = app_config.get("env", {})
    loaded = []
    for key, val in env_config.items():
        # Only set if not already defined in the environment
        if os.environ.get(key) is None:
            # Serialize lists/dicts to JSON strings, primitives to str
            os.environ[key] = json.dumps(val) if isinstance(val, (list, dict)) else str(val)
            loaded.append(key)
    if loaded:
        logger.info(f"Loaded environment variables from config: {loaded}")

    agent_configs = app_config.get("agents", [])
    loaded = load_agents_from_config(agent_configs)
    runnable = loaded.get("runnable", [])

    # Allow overriding specific agent or orchestrator via env vars
    run_agent_name = os.getenv("RUN_AGENT_NAME")
    run_orch_name = os.getenv("RUN_ORCHESTRATOR_NAME")
    if run_agent_name:
        logger.info(f"RUN_AGENT_NAME set, restricting to agent: {run_agent_name}")
        runnable = [a for a in runnable if a.name == run_agent_name]
    
    # Load orchestrator definitions
    orchestrator_configs = app_config.get("orchestrators", [])
    orchestrators = load_orchestrators(orchestrator_configs, loaded)
    if run_orch_name:
        logger.info(f"RUN_ORCHESTRATOR_NAME set, restricting to orchestrator: {run_orch_name}")

    # Partition individual agents
    run_once_agents = [a for a in runnable if a.execution_mode == "run_once"]
    cont_agents = [a for a in runnable if a.execution_mode in ("continuous_loop", "event_driven_kafka")]

    # Partition orchestrators by execution_mode, guard missing attributes
    pipeline_orchestrators = [o for o in orchestrators if getattr(o, 'execution_mode', None) == "run_once_pipeline"]
    manager_orchestrators = [o for o in orchestrators if getattr(o, 'execution_mode', None) == "manage_continuous"]

    # Execute batch agents
    for agent in run_once_agents:
        logger.info(f"Running one-shot agent: {agent.name}")
        # Skip agents that don't implement run_once()
        if not hasattr(agent, 'run_once'):
            logger.warning(f"Skipping agent {agent.name}: no run_once() method")
            continue
        try:
            agent.run_once()
        except Exception as e:
            logger.error(f"Error in run_once agent {agent.name}: {e}", exc_info=True)
    # Execute run-once pipelines
    for orch in pipeline_orchestrators:
        logger.info(f"Running pipeline orchestrator: {orch.name}")
        try:
            orch.run_once()
        except Exception as e:
            logger.error(f"Error in pipeline orchestrator {orch.name}: {e}", exc_info=True)

    # Setup graceful shutdown
    import threading, signal, time
    stop_event = threading.Event()
    def _shutdown(signum, frame):
        logger.info("Shutdown signal received, stopping all agents...")
        stop_event.set()
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Helper to run agent or orchestrator
    def _runner(obj):
        logger.info(f"Starting continuous: {obj.name}")
        try:
            obj.run()
        except Exception as e:
            logger.error(f"{obj.name} crashed: {e}", exc_info=True)
        logger.info(f"{obj.name} stopped")

    # Launch continuous agents and managers
    threads = []
    for obj in cont_agents + manager_orchestrators:
        logger.info(f"Spawning thread for: {obj.name}")
        t = threading.Thread(target=_runner, args=(obj,), name=obj.name)
        threads.append(t)
        t.start()

    # Wait until shutdown
    while not stop_event.is_set():
        time.sleep(1)
    logger.info("Shutting down threads...")
    for t in threads:
        t.join(timeout=5)
    logger.info("All continuous threads stopped. Exiting.")
    return

if __name__ == "__main__":
    main()
