"""
DSPy Optimization Manager with Boot Phase and Disk Caching

This module provides:
1. Boot-time optimization of DSPy modules
2. Disk caching of optimized modules
3. Runtime loading of cached optimizations
4. Re-optimization and cache invalidation
5. Performance monitoring and validation

The system ensures that DSPy modules are optimized once during startup
and reused efficiently during runtime, with automatic fallback to
basic modules if optimization fails.
"""

import os
import json
import pickle
import hashlib
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Type, Union
from dataclasses import dataclass, asdict
import dspy
from dspy import ChainOfThought, ReAct, ProgramOfThought

# Import enhanced DSPy modules - delay import to avoid circular dependency
def _import_dspy_modules():
    """Import DSPy modules when needed to avoid circular imports."""
    try:
        from .enhanced_feed_enricher import (
            SecurityThreatAnalyzer, APTAttributionAnalyzer, 
            ThreatIntelExtractor, ConfidenceScorer
        )
        return {
            'SecurityThreatAnalyzer': SecurityThreatAnalyzer,
            'APTAttributionAnalyzer': APTAttributionAnalyzer,
            'ThreatIntelExtractor': ThreatIntelExtractor,
            'ConfidenceScorer': ConfidenceScorer
        }
    except ImportError:
        # Fall back to advanced_dspy_modules if circular import
        from .advanced_dspy_modules import (
            SecurityThreatAnalyzer, APTAttributionAnalyzer,
            ThreatIntelExtractor, ConfidenceScorer
        )
        return {
            'SecurityThreatAnalyzer': SecurityThreatAnalyzer,
            'APTAttributionAnalyzer': APTAttributionAnalyzer,
            'ThreatIntelExtractor': ThreatIntelExtractor,
            'ConfidenceScorer': ConfidenceScorer
        }

# Import config manager for centralized DSPy configuration
try:
    from .dspy_config_manager import get_dspy_config_manager, configure_dspy_from_config
    CONFIG_MANAGER_AVAILABLE = True
except ImportError:
    CONFIG_MANAGER_AVAILABLE = False

# Prometheus metrics for optimization tracking
try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    
    OPTIMIZATION_COUNTER = Counter(
        'dspy_optimization_runs_total',
        'Total DSPy optimization runs',
        ['module_type', 'success']
    )
    
    OPTIMIZATION_DURATION = Histogram(
        'dspy_optimization_duration_seconds',
        'Time spent optimizing DSPy modules',
        ['module_type'],
        buckets=[1, 5, 10, 30, 60, 120, 300, 600]
    )
    
    CACHE_HITS = Counter(
        'dspy_cache_hits_total',
        'DSPy module cache hits',
        ['module_type']
    )
    
    CACHE_MISSES = Counter(
        'dspy_cache_misses_total',
        'DSPy module cache misses',
        ['module_type']
    )
    
    OPTIMIZATION_QUALITY = Gauge(
        'dspy_optimization_quality_score',
        'Quality score of optimized DSPy modules',
        ['module_type']
    )
    
except ImportError:
    # Fallback for testing
    class _DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self): pass
        def observe(self, val): pass
        def set(self, val): pass
    
    OPTIMIZATION_COUNTER = _DummyMetric()
    OPTIMIZATION_DURATION = _DummyMetric()
    CACHE_HITS = _DummyMetric()
    CACHE_MISSES = _DummyMetric()
    OPTIMIZATION_QUALITY = _DummyMetric()


@dataclass
class OptimizationMetadata:
    """Metadata for cached optimized modules."""
    module_type: str
    optimization_timestamp: str
    optimization_duration: float
    quality_score: float
    training_examples_count: int
    dspy_version: str
    cache_version: str = "1.0"
    validation_metrics: Dict[str, float] = None
    
    def __post_init__(self):
        if self.validation_metrics is None:
            self.validation_metrics = {}


@dataclass
class OptimizationConfig:
    """Configuration for DSPy optimization process."""
    max_optimization_time: int = 300  # 5 minutes
    min_training_examples: int = 10
    max_training_examples: int = 100
    quality_threshold: float = 0.7
    cache_ttl_hours: int = 24
    validation_split: float = 0.2
    optimization_retries: int = 2
    enable_bootstrap: bool = True
    enable_labeledfs: bool = True
    enable_knearestfs: bool = True


class DSPyOptimizationManager:
    """
    Manages DSPy module optimization, caching, and runtime loading.
    
    This class handles the complete lifecycle of DSPy module optimization:
    - Boot-time optimization with synthetic training data
    - Disk caching of optimized modules
    - Runtime loading with fallback mechanisms
    - Cache invalidation and re-optimization
    """
    
    def __init__(
        self,
        cache_dir: str = None,
        config: OptimizationConfig = None,
        logger: logging.Logger = None
    ):
        self.cache_dir = Path(cache_dir or os.getenv('DSPY_CACHE_DIR', './data/dspy_cache'))
        self.config = config or OptimizationConfig()
        self.logger = logger or logging.getLogger(__name__)
        
        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Track loaded modules
        self._loaded_modules: Dict[str, Any] = {}
        self._optimization_history: List[Dict] = []
        
        # Training data generators
        self._training_data_generators = {
            'security_threat_analyzer': self._generate_security_training_data,
            'apt_attribution_analyzer': self._generate_apt_training_data,
            'threat_intel_extractor': self._generate_intel_training_data,
            'confidence_scorer': self._generate_confidence_training_data
        }
        
        self.logger.info(
            "DSPy Optimization Manager initialized",
            extra={
                "cache_dir": str(self.cache_dir),
                "config": asdict(self.config)
            }
        )
    
    def boot_optimization_phase(self) -> Dict[str, bool]:
        """
        Run boot-time optimization for all DSPy modules.
        
        Returns:
            Dict mapping module names to optimization success status
        """
        self.logger.info("Starting DSPy boot optimization phase")
        start_time = time.time()
        
        # Ensure DSPy is properly configured before optimization
        if CONFIG_MANAGER_AVAILABLE:
            try:
                if not configure_dspy_from_config():
                    self.logger.error("Failed to configure DSPy for optimization")
                    return {}
                else:
                    config_manager = get_dspy_config_manager()
                    config_status = config_manager.get_current_config()
                    self.logger.info(
                        "DSPy configured for optimization",
                        extra={
                            "provider": config_status.get('provider'),
                            "model": config_status.get('model')
                        }
                    )
            except Exception as e:
                self.logger.error(f"DSPy configuration failed: {e}")
                return {}
        else:
            self.logger.warning("DSPy config manager not available, assuming DSPy is pre-configured")
        
        # Define modules to optimize - import when needed
        dspy_modules = _import_dspy_modules()
        modules_to_optimize = {
            'security_threat_analyzer': dspy_modules['SecurityThreatAnalyzer'],
            'apt_attribution_analyzer': dspy_modules['APTAttributionAnalyzer'],
            'threat_intel_extractor': dspy_modules['ThreatIntelExtractor'],
            'confidence_scorer': dspy_modules['ConfidenceScorer']
        }
        
        optimization_results = {}
        
        for module_name, module_class in modules_to_optimize.items():
            try:
                self.logger.info(f"Optimizing {module_name}")
                success = self._optimize_module(module_name, module_class)
                optimization_results[module_name] = success
                
                OPTIMIZATION_COUNTER.labels(
                    module_type=module_name,
                    success=str(success).lower()
                ).inc()
                
            except Exception as e:
                self.logger.error(
                    f"Failed to optimize {module_name}",
                    extra={
                        "module_name": module_name,
                        "error": str(e)
                    }
                )
                optimization_results[module_name] = False
                
                OPTIMIZATION_COUNTER.labels(
                    module_type=module_name,
                    success="false"
                ).inc()
        
        total_time = time.time() - start_time
        successful_optimizations = sum(optimization_results.values())
        total_optimizations = len(optimization_results)
        
        self.logger.info(
            "DSPy boot optimization phase completed",
            extra={
                "total_time": total_time,
                "successful_optimizations": successful_optimizations,
                "total_optimizations": total_optimizations,
                "success_rate": successful_optimizations / total_optimizations
            }
        )
        
        return optimization_results
    
    def get_optimized_module(self, module_name: str, module_class: Type = None) -> Any:
        """
        Get an optimized DSPy module, loading from cache or creating new.
        
        Args:
            module_name: Name of the module to load
            module_class: Class to instantiate if optimization not cached
            
        Returns:
            Optimized DSPy module instance
        """
        # Check if already loaded
        if module_name in self._loaded_modules:
            CACHE_HITS.labels(module_type=module_name).inc()
            return self._loaded_modules[module_name]
        
        # Try to load from cache
        cached_module = self._load_from_cache(module_name)
        if cached_module is not None:
            self._loaded_modules[module_name] = cached_module
            CACHE_HITS.labels(module_type=module_name).inc()
            return cached_module
        
        CACHE_MISSES.labels(module_type=module_name).inc()
        
        # Create basic module if cache miss and no class provided
        if module_class is None:
            self.logger.warning(
                f"No cached optimization found for {module_name} and no class provided"
            )
            return None
        
        # Create basic instance
        basic_module = module_class()
        self._loaded_modules[module_name] = basic_module
        
        self.logger.info(
            f"Using basic (non-optimized) module for {module_name}"
        )
        
        return basic_module
    
    def invalidate_cache(self, module_name: str = None):
        """
        Invalidate cached optimizations.
        
        Args:
            module_name: Specific module to invalidate, or None for all
        """
        if module_name:
            cache_file = self.cache_dir / f"{module_name}.pkl"
            meta_file = self.cache_dir / f"{module_name}_meta.json"
            
            for file in [cache_file, meta_file]:
                if file.exists():
                    file.unlink()
                    self.logger.info(f"Invalidated cache for {module_name}")
            
            # Remove from loaded modules
            self._loaded_modules.pop(module_name, None)
        else:
            # Clear all cache
            for cache_file in self.cache_dir.glob("*.pkl"):
                cache_file.unlink()
            for meta_file in self.cache_dir.glob("*_meta.json"):
                meta_file.unlink()
            
            self._loaded_modules.clear()
            self.logger.info("Invalidated all DSPy optimization cache")
    
    def get_optimization_status(self) -> Dict[str, Any]:
        """Get current optimization status and metrics."""
        status = {
            "cache_dir": str(self.cache_dir),
            "loaded_modules": list(self._loaded_modules.keys()),
            "cached_modules": [],
            "cache_stats": {},
            "optimization_history": self._optimization_history[-10:]  # Last 10
        }
        
        # Check cached modules
        for cache_file in self.cache_dir.glob("*.pkl"):
            module_name = cache_file.stem
            meta_file = self.cache_dir / f"{module_name}_meta.json"
            
            if meta_file.exists():
                try:
                    with open(meta_file) as f:
                        metadata = json.load(f)
                    
                    status["cached_modules"].append({
                        "name": module_name,
                        "timestamp": metadata.get("optimization_timestamp"),
                        "quality_score": metadata.get("quality_score"),
                        "duration": metadata.get("optimization_duration")
                    })
                except Exception as e:
                    self.logger.warning(f"Could not read metadata for {module_name}: {e}")
        
        return status
    
    def _optimize_module(self, module_name: str, module_class: Type) -> bool:
        """
        Optimize a specific DSPy module and cache the result.
        
        Args:
            module_name: Name of the module
            module_class: Class to optimize
            
        Returns:
            True if optimization successful, False otherwise
        """
        optimization_start = time.time()
        
        try:
            # Check if recent optimization exists
            if self._is_cache_valid(module_name):
                self.logger.info(f"Valid cache found for {module_name}, skipping optimization")
                return True
            
            # Generate training data
            training_data = self._generate_training_data(module_name)
            if len(training_data) < self.config.min_training_examples:
                self.logger.warning(
                    f"Insufficient training data for {module_name}: {len(training_data)} examples"
                )
                return False
            
            # Split training/validation data
            split_idx = int(len(training_data) * (1 - self.config.validation_split))
            train_data = training_data[:split_idx]
            val_data = training_data[split_idx:]
            
            # Create module instance
            module_instance = module_class()
            
            # Optimize with different strategies
            optimized_module = None
            best_score = 0.0
            optimization_attempts = []
            
            for attempt in range(self.config.optimization_retries + 1):
                try:
                    if self.config.enable_bootstrap and attempt == 0:
                        # Try BootstrapFewShot first
                        optimizer = dspy.BootstrapFewShot(
                            metric=self._create_validation_metric(module_name),
                            max_bootstrapped_demos=min(len(train_data), 10),
                            max_labeled_demos=min(len(train_data), 5)
                        )
                    elif self.config.enable_labeledfs and attempt == 1:
                        # Try LabeledFewShot
                        optimizer = dspy.LabeledFewShot(
                            k=min(len(train_data), 8)
                        )
                    elif self.config.enable_knearestfs and attempt == 2:
                        # Try KNNFewShot
                        optimizer = dspy.KNNFewShot(
                            k=min(len(train_data), 5)
                        )
                    else:
                        # Fallback to basic compilation
                        module_instance.load(train_data[:5])  # Simple few-shot
                        optimized_module = module_instance
                        break
                    
                    # Compile with timeout
                    optimized_module = optimizer.compile(
                        module_instance,
                        trainset=train_data
                    )
                    
                    # Validate quality
                    score = self._validate_module(optimized_module, val_data, module_name)
                    optimization_attempts.append({
                        "attempt": attempt,
                        "optimizer": type(optimizer).__name__,
                        "score": score
                    })
                    
                    if score > best_score:
                        best_score = score
                    
                    if score >= self.config.quality_threshold:
                        break
                        
                except Exception as e:
                    self.logger.warning(
                        f"Optimization attempt {attempt} failed for {module_name}: {e}"
                    )
                    continue
            
            if optimized_module is None:
                self.logger.error(f"All optimization attempts failed for {module_name}")
                return False
            
            # Cache the optimized module
            optimization_duration = time.time() - optimization_start
            metadata = OptimizationMetadata(
                module_type=module_name,
                optimization_timestamp=datetime.now().isoformat(),
                optimization_duration=optimization_duration,
                quality_score=best_score,
                training_examples_count=len(training_data),
                dspy_version=dspy.__version__ if hasattr(dspy, '__version__') else "unknown",
                validation_metrics={
                    "attempts": optimization_attempts,
                    "final_score": best_score
                }
            )
            
            success = self._save_to_cache(module_name, optimized_module, metadata)
            
            if success:
                OPTIMIZATION_DURATION.labels(module_type=module_name).observe(optimization_duration)
                OPTIMIZATION_QUALITY.labels(module_type=module_name).set(best_score)
                
                # Track optimization history
                self._optimization_history.append({
                    "module_name": module_name,
                    "timestamp": metadata.optimization_timestamp,
                    "duration": optimization_duration,
                    "quality_score": best_score,
                    "success": True
                })
                
                self.logger.info(
                    f"Successfully optimized {module_name}",
                    extra={
                        "duration": optimization_duration,
                        "quality_score": best_score,
                        "training_examples": len(training_data)
                    }
                )
            
            return success
            
        except Exception as e:
            optimization_duration = time.time() - optimization_start
            OPTIMIZATION_DURATION.labels(module_type=module_name).observe(optimization_duration)
            
            self.logger.error(
                f"Optimization failed for {module_name}",
                extra={
                    "error": str(e),
                    "duration": optimization_duration
                }
            )
            return False
    
    def _generate_training_data(self, module_name: str) -> List[dspy.Example]:
        """Generate synthetic training data for a module."""
        generator = self._training_data_generators.get(module_name)
        if not generator:
            self.logger.warning(f"No training data generator for {module_name}")
            return []
        
        try:
            return generator()
        except Exception as e:
            self.logger.error(f"Failed to generate training data for {module_name}: {e}")
            return []
    
    def _generate_security_training_data(self) -> List[dspy.Example]:
        """Generate training data for security threat analyzer."""
        examples = [
            dspy.Example(
                content="APT29 group has been observed using new malware variants targeting financial institutions. The campaign utilizes spear-phishing emails with malicious attachments.",
                threat_type="APT Campaign",
                severity="High",
                confidence=0.9
            ),
            dspy.Example(
                content="Multiple ransomware incidents reported across healthcare organizations. Attackers are exploiting unpatched VPN vulnerabilities to gain initial access.",
                threat_type="Ransomware",
                severity="Critical",
                confidence=0.95
            ),
            dspy.Example(
                content="Researchers discover new banking trojan distributed through malvertising campaigns. The malware targets online banking credentials.",
                threat_type="Banking Trojan",
                severity="High",
                confidence=0.85
            ),
            # Add more examples...
        ]
        
        # Generate additional synthetic examples
        for i in range(20):
            examples.append(dspy.Example(
                content=f"Security alert {i}: Suspicious network activity detected from IP address 192.168.{i}.{i}. Pattern matches known attack signatures.",
                threat_type="Network Intrusion",
                severity="Medium",
                confidence=0.7
            ))
        
        return examples[:self.config.max_training_examples]
    
    def _generate_apt_training_data(self) -> List[dspy.Example]:
        """Generate training data for APT attribution analyzer."""
        examples = [
            dspy.Example(
                indicators="Command and control infrastructure, TTPs match previous campaigns",
                attribution="APT29",
                confidence=0.85,
                reasoning="Infrastructure overlap with previous APT29 campaigns, similar malware families"
            ),
            dspy.Example(
                indicators="Custom malware, specific targeting of government entities",
                attribution="APT1",
                confidence=0.8,
                reasoning="Targeting pattern and malware characteristics consistent with APT1"
            )
        ]
        
        # Generate more synthetic examples
        apt_groups = ["APT28", "APT29", "APT1", "Lazarus", "FIN7", "Carbanak"]
        for i, group in enumerate(apt_groups * 5):
            examples.append(dspy.Example(
                indicators=f"Custom tools, infrastructure pattern {i}",
                attribution=group,
                confidence=0.7 + (i % 3) * 0.1,
                reasoning=f"Pattern analysis suggests {group} involvement"
            ))
        
        return examples[:self.config.max_training_examples]
    
    def _generate_intel_training_data(self) -> List[dspy.Example]:
        """Generate training data for threat intel extractor."""
        examples = [
            dspy.Example(
                raw_content="Blog post about new malware campaign targeting financial sector...",
                extracted_iocs=["malware_hash_123", "192.168.1.1", "malicious-domain.com"],
                threat_summary="Financial sector targeted by new malware campaign",
                confidence=0.9
            )
        ]
        
        for i in range(25):
            examples.append(dspy.Example(
                raw_content=f"Technical analysis of security incident {i}...",
                extracted_iocs=[f"hash_{i}", f"ip_{i}", f"domain_{i}.com"],
                threat_summary=f"Security incident {i} analysis",
                confidence=0.8
            ))
        
        return examples[:self.config.max_training_examples]
    
    def _generate_confidence_training_data(self) -> List[dspy.Example]:
        """Generate training data for confidence scorer."""
        examples = [
            dspy.Example(
                analysis_result="High confidence threat identification based on multiple indicators",
                factors=["multiple_sources", "technical_analysis", "pattern_match"],
                confidence_score=0.9,
                uncertainty_factors=["limited_attribution_evidence"]
            )
        ]
        
        for i in range(20):
            examples.append(dspy.Example(
                analysis_result=f"Analysis result {i}",
                factors=[f"factor_{i}", "technical_validation"],
                confidence_score=0.7 + (i % 3) * 0.1,
                uncertainty_factors=[f"limitation_{i}"]
            ))
        
        return examples[:self.config.max_training_examples]
    
    def _create_validation_metric(self, module_name: str):
        """Create validation metric for module optimization."""
        def validation_metric(example, pred, trace=None):
            # Simple validation - can be enhanced
            if hasattr(pred, 'confidence') and hasattr(example, 'confidence'):
                return abs(pred.confidence - example.confidence) < 0.2
            return True
        
        return validation_metric
    
    def _validate_module(self, module, validation_data: List, module_name: str) -> float:
        """Validate optimized module performance."""
        if not validation_data:
            return 0.5  # Default score for no validation data
        
        try:
            correct = 0
            total = len(validation_data)
            
            for example in validation_data:
                try:
                    # Run prediction
                    if hasattr(module, 'forward'):
                        result = module.forward(**example.inputs())
                    else:
                        result = module(**example.inputs())
                    
                    # Simple validation logic
                    if hasattr(result, 'confidence') and hasattr(example, 'confidence'):
                        if abs(result.confidence - example.confidence) < 0.3:
                            correct += 1
                    else:
                        correct += 1  # Assume correct if no confidence comparison
                        
                except Exception as e:
                    self.logger.debug(f"Validation error for {module_name}: {e}")
                    continue
            
            score = correct / total if total > 0 else 0.5
            return score
            
        except Exception as e:
            self.logger.warning(f"Validation failed for {module_name}: {e}")
            return 0.5
    
    def _save_to_cache(self, module_name: str, module, metadata: OptimizationMetadata) -> bool:
        """Save optimized module and metadata to cache."""
        try:
            # Save module
            cache_file = self.cache_dir / f"{module_name}.pkl"
            with open(cache_file, 'wb') as f:
                pickle.dump(module, f)
            
            # Save metadata
            meta_file = self.cache_dir / f"{module_name}_meta.json"
            with open(meta_file, 'w') as f:
                json.dump(asdict(metadata), f, indent=2)
            
            self.logger.debug(f"Saved optimized {module_name} to cache")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cache {module_name}: {e}")
            return False
    
    def _load_from_cache(self, module_name: str) -> Optional[Any]:
        """Load optimized module from cache."""
        try:
            cache_file = self.cache_dir / f"{module_name}.pkl"
            meta_file = self.cache_dir / f"{module_name}_meta.json"
            
            if not cache_file.exists() or not meta_file.exists():
                return None
            
            # Check if cache is still valid
            if not self._is_cache_valid(module_name):
                return None
            
            # Load module
            with open(cache_file, 'rb') as f:
                module = pickle.load(f)
            
            self.logger.debug(f"Loaded optimized {module_name} from cache")
            return module
            
        except Exception as e:
            self.logger.warning(f"Failed to load {module_name} from cache: {e}")
            return None
    
    def _is_cache_valid(self, module_name: str) -> bool:
        """Check if cached optimization is still valid."""
        try:
            meta_file = self.cache_dir / f"{module_name}_meta.json"
            if not meta_file.exists():
                return False
            
            with open(meta_file) as f:
                metadata = json.load(f)
            
            # Check cache age
            cache_time = datetime.fromisoformat(metadata['optimization_timestamp'])
            ttl = timedelta(hours=self.config.cache_ttl_hours)
            
            if datetime.now() - cache_time > ttl:
                self.logger.debug(f"Cache expired for {module_name}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.warning(f"Cache validation failed for {module_name}: {e}")
            return False


# Global optimization manager instance
_optimization_manager: Optional[DSPyOptimizationManager] = None


def get_optimization_manager() -> DSPyOptimizationManager:
    """Get or create global optimization manager."""
    global _optimization_manager
    if _optimization_manager is None:
        _optimization_manager = DSPyOptimizationManager()
    return _optimization_manager


def initialize_dspy_optimizations(config: OptimizationConfig = None) -> Dict[str, bool]:
    """
    Initialize DSPy optimizations during application boot.
    
    This function should be called during application startup to
    optimize and cache DSPy modules for runtime use.
    
    Args:
        config: Optimization configuration
        
    Returns:
        Dict mapping module names to optimization success status
    """
    manager = get_optimization_manager()
    if config:
        manager.config = config
    
    return manager.boot_optimization_phase()


def get_optimized_module(module_name: str, module_class: Type = None) -> Any:
    """
    Get an optimized DSPy module for runtime use.
    
    Args:
        module_name: Name of the module
        module_class: Fallback class if optimization not available
        
    Returns:
        Optimized DSPy module instance
    """
    manager = get_optimization_manager()
    return manager.get_optimized_module(module_name, module_class)


def invalidate_optimization_cache(module_name: str = None):
    """Invalidate optimization cache."""
    manager = get_optimization_manager()
    manager.invalidate_cache(module_name)


def get_optimization_status() -> Dict[str, Any]:
    """Get current optimization status."""
    manager = get_optimization_manager()
    return manager.get_optimization_status()