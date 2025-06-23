#!/usr/bin/env python3
"""
DSPy Boot Optimization Script

This script runs DSPy module optimizations during application startup.
It should be executed before starting the main application to ensure
optimized modules are cached and ready for runtime use.

Usage:
    python dspy_boot_optimizer.py [options]
    
Environment Variables:
    DSPY_CACHE_DIR: Directory for caching optimized modules (default: ./data/dspy_cache)
    DSPY_OPTIMIZATION_TIMEOUT: Maximum time for optimization phase in seconds (default: 600)
    DSPY_SKIP_OPTIMIZATION: Set to 'true' to skip optimization (use cached only)
    DSPY_FORCE_REOPTIMIZATION: Set to 'true' to force re-optimization
"""

import os
import sys
import time
import logging
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common_tools.dspy_optimization_manager import (
    DSPyOptimizationManager, OptimizationConfig, 
    initialize_dspy_optimizations, get_optimization_status
)
from common_tools.structured_logging import setup_agent_logging


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Setup logging for optimization script."""
    logger = setup_agent_logging(
        agent_name="dspy_boot_optimizer",
        log_level=getattr(logging, log_level.upper()),
        log_file="logs/dspy_optimization.log"
    )
    return logger


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="DSPy Boot Optimization Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings
  python dspy_boot_optimizer.py
  
  # Run with custom cache directory and timeout
  python dspy_boot_optimizer.py --cache-dir /opt/dspy_cache --timeout 300
  
  # Force re-optimization
  python dspy_boot_optimizer.py --force-reoptimization
  
  # Skip optimization, use cache only
  python dspy_boot_optimizer.py --skip-optimization
  
  # Dry run to check status
  python dspy_boot_optimizer.py --status-only
        """
    )
    
    parser.add_argument(
        '--cache-dir',
        default=os.getenv('DSPY_CACHE_DIR', './data/dspy_cache'),
        help='Directory for caching optimized modules'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=int(os.getenv('DSPY_OPTIMIZATION_TIMEOUT', '600')),
        help='Maximum time for optimization phase in seconds'
    )
    
    parser.add_argument(
        '--max-training-examples',
        type=int,
        default=50,
        help='Maximum training examples per module'
    )
    
    parser.add_argument(
        '--quality-threshold',
        type=float,
        default=0.7,
        help='Minimum quality threshold for optimization'
    )
    
    parser.add_argument(
        '--cache-ttl-hours',
        type=int,
        default=24,
        help='Cache TTL in hours'
    )
    
    parser.add_argument(
        '--skip-optimization',
        action='store_true',
        default=os.getenv('DSPY_SKIP_OPTIMIZATION', '').lower() == 'true',
        help='Skip optimization, use cached modules only'
    )
    
    parser.add_argument(
        '--force-reoptimization',
        action='store_true',
        default=os.getenv('DSPY_FORCE_REOPTIMIZATION', '').lower() == 'true',
        help='Force re-optimization even if cache is valid'
    )
    
    parser.add_argument(
        '--status-only',
        action='store_true',
        help='Show optimization status and exit'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Log level'
    )
    
    parser.add_argument(
        '--metrics-port',
        type=int,
        default=8002,
        help='Port for optimization metrics server'
    )
    
    return parser.parse_args()


def check_prerequisites(logger: logging.Logger) -> bool:
    """Check if prerequisites for optimization are met."""
    logger.info("Checking optimization prerequisites")
    
    try:
        # Check DSPy import
        import dspy
        logger.info(f"DSPy version: {getattr(dspy, '__version__', 'unknown')}")
        
        # Check if LLM is configured
        if not hasattr(dspy.settings, 'lm') or dspy.settings.lm is None:
            logger.warning("No LLM configured in DSPy settings")
            
            # Use the centralized DSPy configuration manager
            try:
                from common_tools.dspy_config_manager import configure_dspy_from_config
                
                if configure_dspy_from_config():
                    logger.info("Successfully configured DSPy using configuration manager")
                else:
                    logger.error("Failed to configure DSPy - no valid API keys or providers")
                    return False
                    
            except ImportError:
                # Fallback to manual configuration if config manager not available
                logger.warning("DSPy config manager not available, using fallback configuration")
                
                api_key = os.getenv('OPENAI_API_KEY') or os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
                if api_key:
                    if os.getenv('OPENAI_API_KEY'):
                        from dspy import OpenAI
                        lm = OpenAI(api_key=api_key, model="gpt-4-turbo-preview")
                    else:
                        from dspy import GoogleGenerativeAI
                        lm = GoogleGenerativeAI(model="gemini-1.5-pro", api_key=api_key)
                    
                    dspy.settings.configure(lm=lm)
                    logger.info("Configured LLM using fallback method")
                else:
                    logger.error("No LLM API key found in environment")
                    return False
        
        # Check enhanced modules
        from common_tools.enhanced_feed_enricher import (
            SecurityThreatAnalyzer, APTAttributionAnalyzer,
            ThreatIntelExtractor, ConfidenceScorer
        )
        logger.info("Enhanced DSPy modules available")
        
        return True
        
    except ImportError as e:
        logger.error(f"Import error: {e}")
        return False
    except Exception as e:
        logger.error(f"Prerequisites check failed: {e}")
        return False


def run_optimization(config: OptimizationConfig, logger: logging.Logger) -> bool:
    """Run the optimization process."""
    logger.info("Starting DSPy optimization process")
    
    start_time = time.time()
    try:
        # Initialize optimization manager
        manager = DSPyOptimizationManager(
            cache_dir=config.cache_dir if hasattr(config, 'cache_dir') else None,
            config=config,
            logger=logger
        )
        
        # Run boot optimization
        results = manager.boot_optimization_phase()
        
        # Log results
        total_modules = len(results)
        successful_modules = sum(results.values())
        failed_modules = total_modules - successful_modules
        
        optimization_time = time.time() - start_time
        
        logger.info(
            "Optimization process completed",
            extra={
                "total_modules": total_modules,
                "successful_modules": successful_modules,
                "failed_modules": failed_modules,
                "success_rate": successful_modules / total_modules if total_modules > 0 else 0,
                "optimization_time": optimization_time
            }
        )
        
        # Print summary
        print("\n" + "="*60)
        print("DSPy Optimization Summary")
        print("="*60)
        print(f"Total modules: {total_modules}")
        print(f"Successfully optimized: {successful_modules}")
        print(f"Failed: {failed_modules}")
        print(f"Success rate: {successful_modules/total_modules*100:.1f}%")
        print(f"Total time: {optimization_time:.1f} seconds")
        print("="*60)
        
        # Show detailed results
        for module_name, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"{module_name}: {status}")
        
        return successful_modules > 0
        
    except Exception as e:
        logger.error(f"Optimization process failed: {e}")
        return False


def show_status(cache_dir: str, logger: logging.Logger):
    """Show current optimization status."""
    try:
        manager = DSPyOptimizationManager(cache_dir=cache_dir, logger=logger)
        status = manager.get_optimization_status()
        
        print("\n" + "="*60)
        print("DSPy Optimization Status")
        print("="*60)
        print(f"Cache directory: {status['cache_dir']}")
        print(f"Loaded modules: {len(status['loaded_modules'])}")
        print(f"Cached modules: {len(status['cached_modules'])}")
        
        if status['cached_modules']:
            print("\nCached Modules:")
            for module in status['cached_modules']:
                print(f"  {module['name']}: {module['quality_score']:.2f} quality, "
                      f"optimized {module['timestamp']}")
        
        if status['loaded_modules']:
            print(f"\nLoaded modules: {', '.join(status['loaded_modules'])}")
        
        if status['optimization_history']:
            print("\nRecent Optimizations:")
            for entry in status['optimization_history'][-5:]:
                print(f"  {entry['module_name']}: {entry['quality_score']:.2f} quality, "
                      f"{entry['duration']:.1f}s")
        
        print("="*60)
        
    except Exception as e:
        logger.error(f"Failed to get status: {e}")


def main():
    """Main execution function."""
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logging(args.log_level)
    
    # Start metrics server if requested
    if args.metrics_port:
        try:
            from prometheus_client import start_http_server
            start_http_server(args.metrics_port)
            logger.info(f"Started metrics server on port {args.metrics_port}")
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")
    
    logger.info(
        "DSPy Boot Optimizer started",
        extra={
            "cache_dir": args.cache_dir,
            "timeout": args.timeout,
            "skip_optimization": args.skip_optimization,
            "force_reoptimization": args.force_reoptimization
        }
    )
    
    # Show status if requested
    if args.status_only:
        show_status(args.cache_dir, logger)
        return 0
    
    # Check prerequisites
    if not check_prerequisites(logger):
        logger.error("Prerequisites not met, cannot proceed with optimization")
        return 1
    
    # Create optimization config
    config = OptimizationConfig(
        max_optimization_time=args.timeout,
        max_training_examples=args.max_training_examples,
        quality_threshold=args.quality_threshold,
        cache_ttl_hours=args.cache_ttl_hours
    )
    config.cache_dir = args.cache_dir
    
    # Handle force re-optimization
    if args.force_reoptimization:
        logger.info("Force re-optimization requested, clearing cache")
        manager = DSPyOptimizationManager(cache_dir=args.cache_dir, logger=logger)
        manager.invalidate_cache()
    
    # Skip optimization if requested
    if args.skip_optimization:
        logger.info("Skipping optimization, using cached modules only")
        show_status(args.cache_dir, logger)
        return 0
    
    # Run optimization
    success = run_optimization(config, logger)
    
    if success:
        logger.info("DSPy boot optimization completed successfully")
        return 0
    else:
        logger.error("DSPy boot optimization failed")
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nOptimization interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)