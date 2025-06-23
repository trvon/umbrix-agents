#!/usr/bin/env python3
"""
Migration script to integrate Enhanced RSS Collector with existing system.

This script:
1. Updates configuration files to use the enhanced collector
2. Creates backup of existing configuration
3. Updates Docker Compose and systemd services
4. Provides rollback capability
5. Validates the integration

Usage:
    python migrate_to_enhanced_rss_collector.py --dry-run   # Preview changes
    python migrate_to_enhanced_rss_collector.py --apply     # Apply changes
    python migrate_to_enhanced_rss_collector.py --rollback  # Rollback changes
"""

import os
import sys
import shutil
import json
import yaml
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from collector_agent.enhanced_rss_collector import EnhancedRssCollectorAgent
from collector_agent.rss_collector import RssCollectorAgent
from common_tools.intelligent_content_analyzer import IntelligentContentAnalyzer


class MigrationError(Exception):
    """Exception raised during migration process."""
    pass


class EnhancedCollectorMigration:
    """Handles migration to Enhanced RSS Collector."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.backup_dir = Path(f"backups/migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        self.agents_dir = Path(__file__).parent.parent
        self.project_root = self.agents_dir.parent
        
        # Files to update
        self.config_files = [
            self.agents_dir / "config.yaml",
            self.agents_dir / "config" / "retry_config.yaml",
            self.project_root / "docker-compose.yml",
            self.project_root / "docker-compose.override.yml"
        ]
        
        # Service files
        self.service_files = [
            "/etc/systemd/system/umbrix-rss-collector.service"
        ]
        
        print(f"Migration initialized. Dry run: {dry_run}")
        print(f"Backup directory: {self.backup_dir}")
    
    def validate_prerequisites(self) -> bool:
        """Validate that prerequisites are met for migration."""
        print("\n=== Validating Prerequisites ===")
        
        issues = []
        
        # Check if enhanced collector files exist
        enhanced_files = [
            self.agents_dir / "collector_agent" / "enhanced_rss_collector.py",
            self.agents_dir / "common_tools" / "intelligent_content_analyzer.py",
            self.agents_dir / "common_tools" / "enhanced_feed_enricher.py",
            self.agents_dir / "tests" / "test_enhanced_rss_collector.py"
        ]
        
        for file_path in enhanced_files:
            if not file_path.exists():
                issues.append(f"Missing required file: {file_path}")
            else:
                print(f"✓ Found: {file_path}")
        
        # Check if retry config has enhanced policies
        retry_config_path = self.agents_dir / "config" / "retry_config.yaml"
        if retry_config_path.exists():
            try:
                with open(retry_config_path) as f:
                    config = yaml.safe_load(f)
                
                required_policies = ['content_extraction', 'feed_enrichment', 'content_analysis']
                policies = config.get('retry_policies', {})
                
                for policy in required_policies:
                    if policy in policies:
                        print(f"✓ Found retry policy: {policy}")
                    else:
                        issues.append(f"Missing retry policy: {policy}")
                        
            except Exception as e:
                issues.append(f"Error reading retry config: {e}")
        else:
            issues.append(f"Missing retry config: {retry_config_path}")
        
        # Test import of enhanced collector
        try:
            from collector_agent.enhanced_rss_collector import EnhancedRssCollectorAgent
            print("✓ Enhanced RSS Collector can be imported")
        except Exception as e:
            issues.append(f"Cannot import EnhancedRssCollectorAgent: {e}")
        
        # Test content analyzer
        try:
            analyzer = IntelligentContentAnalyzer()
            print("✓ Intelligent Content Analyzer can be instantiated")
        except Exception as e:
            issues.append(f"Cannot create IntelligentContentAnalyzer: {e}")
        
        if issues:
            print(f"\n❌ Found {len(issues)} issues:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        
        print("\n✅ All prerequisites met")
        return True
    
    def create_backup(self) -> bool:
        """Create backup of current configuration."""
        print(f"\n=== Creating Backup ===")
        
        if self.dry_run:
            print("DRY RUN: Would create backup directory and copy files")
            return True
        
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Backup configuration files
            for config_file in self.config_files:
                if config_file.exists():
                    backup_path = self.backup_dir / config_file.name
                    shutil.copy2(config_file, backup_path)
                    print(f"✓ Backed up: {config_file} -> {backup_path}")
            
            # Backup current collector
            current_collector = self.agents_dir / "collector_agent" / "rss_collector.py"
            if current_collector.exists():
                backup_collector = self.backup_dir / "rss_collector_original.py"
                shutil.copy2(current_collector, backup_collector)
                print(f"✓ Backed up: {current_collector} -> {backup_collector}")
            
            # Create migration manifest
            manifest = {
                "migration_date": datetime.now().isoformat(),
                "backup_dir": str(self.backup_dir),
                "files_backed_up": [str(f) for f in self.config_files if f.exists()],
                "migration_type": "enhanced_rss_collector"
            }
            
            manifest_path = self.backup_dir / "migration_manifest.json"
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            print(f"✓ Created migration manifest: {manifest_path}")
            return True
            
        except Exception as e:
            print(f"❌ Backup failed: {e}")
            return False
    
    def update_agent_config(self) -> bool:
        """Update agent configuration to use enhanced collector."""
        print("\n=== Updating Agent Configuration ===")
        
        config_path = self.agents_dir / "config.yaml"
        
        if not config_path.exists():
            print(f"❌ Config file not found: {config_path}")
            return False
        
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f) or {}
            
            # Add enhanced collector configuration
            if 'collectors' not in config:
                config['collectors'] = {}
            
            config['collectors']['rss'] = {
                'class': 'collector_agent.enhanced_rss_collector.EnhancedRssCollectorAgent',
                'use_optimized_enrichment': True,
                'fallback_enabled': True,
                'prometheus_port': 8001  # Different port for enhanced metrics
            }
            
            # Add content analysis configuration
            if 'content_analysis' not in config:
                config['content_analysis'] = {}
            
            config['content_analysis'].update({
                'enabled': True,
                'confidence_threshold': 0.7,
                'security_detection_enabled': True,
                'api_detection_enabled': True,
                'entity_extraction_enabled': True
            })
            
            # Update DSPy configuration for enhanced enrichment
            if 'dspy' not in config:
                config['dspy'] = {}
            
            config['dspy'].update({
                'optimized_modules_enabled': True,
                'confidence_scoring_enabled': True,
                'multi_step_reasoning_enabled': True,
                'uncertainty_quantification_enabled': True
            })
            
            if self.dry_run:
                print("DRY RUN: Would update config.yaml with:")
                print(yaml.dump(config, default_flow_style=False, indent=2))
            else:
                with open(config_path, 'w') as f:
                    yaml.dump(config, f, default_flow_style=False, indent=2)
                print(f"✓ Updated: {config_path}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to update agent config: {e}")
            return False
    
    def update_docker_compose(self) -> bool:
        """Update Docker Compose to use enhanced collector."""
        print("\n=== Updating Docker Compose ===")
        
        compose_path = self.project_root / "docker-compose.yml"
        
        if not compose_path.exists():
            print(f"ℹ️  Docker Compose file not found: {compose_path}")
            return True
        
        try:
            with open(compose_path) as f:
                compose_content = f.read()
            
            # Replace collector module reference
            updated_content = compose_content.replace(
                'collector_agent.rss_collector',
                'collector_agent.enhanced_rss_collector'
            )
            
            # Add enhanced metrics port
            if 'ports:' in updated_content and '8000:8000' in updated_content:
                updated_content = updated_content.replace(
                    '8000:8000',
                    '8000:8000\n      - "8001:8001"  # Enhanced RSS collector metrics'
                )
            
            if self.dry_run:
                print("DRY RUN: Would update docker-compose.yml")
                if updated_content != compose_content:
                    print("Changes detected in Docker Compose configuration")
            else:
                with open(compose_path, 'w') as f:
                    f.write(updated_content)
                print(f"✓ Updated: {compose_path}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to update Docker Compose: {e}")
            return False
    
    def update_systemd_service(self) -> bool:
        """Update systemd service to use enhanced collector."""
        print("\n=== Updating Systemd Service ===")
        
        service_path = Path("/etc/systemd/system/umbrix-rss-collector.service")
        
        if not service_path.exists():
            print(f"ℹ️  Systemd service file not found: {service_path}")
            return True
        
        if not os.access(service_path, os.W_OK):
            print(f"❌ No write permission for: {service_path}")
            return False
        
        try:
            with open(service_path) as f:
                service_content = f.read()
            
            # Replace module reference
            updated_content = service_content.replace(
                'collector_agent.rss_collector',
                'collector_agent.enhanced_rss_collector'
            )
            
            # Add enhanced arguments
            if 'ExecStart=' in updated_content:
                updated_content = updated_content.replace(
                    'ExecStart=',
                    'ExecStart='
                ).replace(
                    'rss_collector',
                    'enhanced_rss_collector --use-optimized --fallback'
                )
            
            if self.dry_run:
                print("DRY RUN: Would update systemd service")
                if updated_content != service_content:
                    print("Changes detected in systemd service configuration")
            else:
                with open(service_path, 'w') as f:
                    f.write(updated_content)
                
                # Reload systemd daemon
                subprocess.run(['systemctl', 'daemon-reload'], check=True)
                print(f"✓ Updated: {service_path}")
                print("✓ Reloaded systemd daemon")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to update systemd service: {e}")
            return False
    
    def run_integration_tests(self) -> bool:
        """Run integration tests to validate the migration."""
        print("\n=== Running Integration Tests ===")
        
        if self.dry_run:
            print("DRY RUN: Would run integration tests")
            return True
        
        try:
            # Test enhanced collector import and initialization
            from collector_agent.enhanced_rss_collector import EnhancedRssCollectorAgent
            
            agent = EnhancedRssCollectorAgent(
                use_kafka=False,  # Test mode
                use_optimized_enrichment=True,
                fallback_enabled=True
            )
            
            print("✓ Enhanced RSS Collector initialized successfully")
            
            # Test content analyzer
            from common_tools.intelligent_content_analyzer import IntelligentContentAnalyzer
            
            analyzer = IntelligentContentAnalyzer()
            test_result = analyzer.analyze_content(
                title="Test Security Alert",
                content="APT29 campaign discovered targeting financial institutions with new malware."
            )
            
            if test_result.content_type == 'security_threat':
                print("✓ Content analyzer correctly identified security content")
            else:
                print(f"⚠️  Content analyzer returned: {test_result.content_type}")
            
            # Run pytest on enhanced collector tests
            test_command = [
                'python', '-m', 'pytest', 
                str(self.agents_dir / "tests" / "test_enhanced_rss_collector.py"),
                '-v', '--tb=short'
            ]
            
            result = subprocess.run(test_command, capture_output=True, text=True, cwd=self.agents_dir)
            
            if result.returncode == 0:
                print("✓ Enhanced collector tests passed")
            else:
                print(f"❌ Tests failed:\n{result.stdout}\n{result.stderr}")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ Integration tests failed: {e}")
            return False
    
    def apply_migration(self) -> bool:
        """Apply the complete migration."""
        print("\n🚀 Starting Enhanced RSS Collector Migration")
        
        if not self.validate_prerequisites():
            return False
        
        if not self.create_backup():
            return False
        
        if not self.update_agent_config():
            return False
        
        if not self.update_docker_compose():
            return False
        
        if not self.update_systemd_service():
            return False
        
        if not self.run_integration_tests():
            return False
        
        print("\n✅ Migration completed successfully!")
        print("\nNext steps:")
        print("1. Restart the RSS collector service:")
        print("   docker-compose restart rss-collector")
        print("   # OR")
        print("   systemctl restart umbrix-rss-collector")
        print("\n2. Monitor enhanced metrics at:")
        print("   http://localhost:8001/metrics")
        print("\n3. Review logs for enhanced enrichment activity")
        print(f"\n4. Backup location: {self.backup_dir}")
        
        return True
    
    def rollback_migration(self) -> bool:
        """Rollback the migration using the most recent backup."""
        print("\n🔄 Rolling back Enhanced RSS Collector Migration")
        
        # Find most recent backup
        backups_dir = Path("backups")
        if not backups_dir.exists():
            print("❌ No backups directory found")
            return False
        
        migration_backups = [d for d in backups_dir.iterdir() if d.is_dir() and d.name.startswith('migration_')]
        if not migration_backups:
            print("❌ No migration backups found")
            return False
        
        latest_backup = max(migration_backups, key=lambda x: x.name)
        print(f"Using backup: {latest_backup}")
        
        # Read manifest
        manifest_path = latest_backup / "migration_manifest.json"
        if not manifest_path.exists():
            print("❌ Migration manifest not found")
            return False
        
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
            
            # Restore files
            for backed_up_file in manifest['files_backed_up']:
                source_path = latest_backup / Path(backed_up_file).name
                target_path = Path(backed_up_file)
                
                if source_path.exists():
                    if self.dry_run:
                        print(f"DRY RUN: Would restore {source_path} -> {target_path}")
                    else:
                        shutil.copy2(source_path, target_path)
                        print(f"✓ Restored: {target_path}")
            
            if not self.dry_run:
                # Reload systemd if service was updated
                try:
                    subprocess.run(['systemctl', 'daemon-reload'], check=True)
                    print("✓ Reloaded systemd daemon")
                except subprocess.CalledProcessError:
                    print("⚠️  Could not reload systemd daemon")
            
            print("\n✅ Rollback completed successfully!")
            print("Restart services to apply the rollback")
            
            return True
            
        except Exception as e:
            print(f"❌ Rollback failed: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="Enhanced RSS Collector Migration Tool")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dry-run', action='store_true', help='Preview changes without applying them')
    group.add_argument('--apply', action='store_true', help='Apply the migration')
    group.add_argument('--rollback', action='store_true', help='Rollback the migration')
    
    args = parser.parse_args()
    
    migration = EnhancedCollectorMigration(dry_run=args.dry_run)
    
    try:
        if args.dry_run or args.apply:
            success = migration.apply_migration()
        elif args.rollback:
            success = migration.rollback_migration()
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()