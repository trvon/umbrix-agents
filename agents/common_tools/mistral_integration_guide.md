# Mistral DSPy Integration Guide

This guide explains how to use Mistral AI models with the DSPy framework in the Umbrix CTI platform.

## Overview

The Mistral integration extends the existing DSPy configuration system to support Mistral AI models alongside OpenAI and Gemini providers. This provides additional options for LLM reasoning and allows for provider diversity and fallback scenarios.

## Features

### Supported Mistral Models

- **mistral-large-latest** (Primary) - Latest large model, best performance
- **mistral-medium-latest** (Fallback) - Medium-sized model, balanced performance
- **mistral-small-latest** (Fast) - Smaller model, fastest responses

### DSPy Integration

- Full DSPy compatibility with existing reasoning modules
- Automatic provider fallback (Mistral → OpenAI → Gemini)
- Support for all DSPy patterns (ChainOfThought, ReAct, etc.)
- Configuration management through environment variables and config files

## Setup

### 1. Install Required Dependencies

```bash
cd agents
uv add mistralai
# OR
pip install mistralai
```

### 2. Set Environment Variables

```bash
# Required: Mistral API key
export MISTRAL_API_KEY="your_mistral_api_key_here"

# Optional: Specific model override
export MISTRAL_MODEL="mistral-large-latest"

# Optional: Force Mistral as primary provider
export DSPY_PROVIDER="mistral"
```

### 3. Update Configuration

Update `agents/config.yaml`:

```yaml
dspy_settings:
  llm_provider: "mistral"
  mistral_model: "mistral-large-latest"
  temperature: 0.2
  max_output_tokens: 2048
```

## Usage

### Basic DSPy Configuration

```python
from common_tools.dspy_config_manager import configure_dspy_from_config

# Auto-configure with Mistral (if available and configured)
success = configure_dspy_from_config()

# Force Mistral specifically
success = configure_dspy_from_config(force_provider="mistral")
```

### Check Configuration Status

```python
from common_tools.dspy_config_manager import get_dspy_status

status = get_dspy_status()
print(f"Provider: {status['provider']}")
print(f"Model: {status['model']}")
print(f"Mistral available: {status['mistral_available']}")
```

### Using Mistral Provider Directly

```python
from common_tools.mistral_dspy_provider import MistralDSPyProvider
import os

# Create provider directly
provider = MistralDSPyProvider(
    model="mistral-large-latest",
    api_key=os.getenv('MISTRAL_API_KEY'),
    temperature=0.2
)

# Test connection
if provider.test_connection():
    print("✅ Mistral connection successful")
    
    # Generate text
    response = provider.generate("Explain threat intelligence in one sentence.")
    print(f"Response: {response[0]}")
```

### DSPy Modules with Mistral

```python
import dspy
from common_tools.dspy_config_manager import configure_dspy_from_config

# Configure DSPy to use Mistral
configure_dspy_from_config(force_provider="mistral")

# Define a signature
class ThreatAnalysis(dspy.Signature):
    threat_description: str = dspy.InputField()
    severity_level: str = dspy.OutputField(desc="Low, Medium, High, Critical")
    mitigation_strategy: str = dspy.OutputField()

# Use with ChainOfThought
threat_analyzer = dspy.ChainOfThought(ThreatAnalysis)

# Analyze a threat
result = threat_analyzer(
    threat_description="Unusual network traffic to external IP addresses with encryption"
)

print(f"Severity: {result.severity_level}")
print(f"Mitigation: {result.mitigation_strategy}")
```

## Provider Fallback System

The system automatically tries providers in this order:

1. **Primary**: Configured provider (Mistral if set)
2. **Secondary**: OpenAI (if API key available)
3. **Tertiary**: Gemini (if API key available)

### Manual Fallback

```python
from common_tools.dspy_config_manager import reconfigure_dspy_with_fallback

# If current provider fails, switch to next available
success = reconfigure_dspy_with_fallback()
if success:
    print("Successfully switched to fallback provider")
```

## Configuration Options

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MISTRAL_API_KEY` | Mistral API key (required) | None |
| `MISTRAL_MODEL` | Specific Mistral model | mistral-large-latest |
| `DSPY_PROVIDER` | Force specific provider | auto-detect |

### Config File Settings

```yaml
dspy_settings:
  llm_provider: "mistral"
  mistral_model: "mistral-large-latest"
  temperature: 0.2
  max_output_tokens: 2048
  mistral_settings:
    temperature: 0.2
    max_tokens: 2048
```

## Model Recommendations

### Use Cases

- **Critical Analysis**: `mistral-large-latest` - Best reasoning capabilities
- **Batch Processing**: `mistral-medium-latest` - Good balance of speed/quality  
- **Real-time Processing**: `mistral-small-latest` - Fastest responses

### Performance Characteristics

| Model | Speed | Quality | Cost | Best For |
|-------|-------|---------|------|----------|
| mistral-large-latest | Slow | Excellent | High | Complex reasoning, critical analysis |
| mistral-medium-latest | Medium | Good | Medium | General purpose, balanced workloads |
| mistral-small-latest | Fast | Fair | Low | Simple tasks, high-volume processing |

## Integration with Existing Modules

### Enhanced RSS Collector

```python
# The enhanced RSS collector automatically uses configured DSPy provider
from collector_agent.enhanced_rss_collector import EnhancedRssCollectorAgent

collector = EnhancedRssCollectorAgent(
    use_optimized_enrichment=True,
    fallback_enabled=True  # Enables automatic provider fallback
)
```

### Intelligent Content Analyzer

```python
from common_tools.intelligent_content_analyzer import IntelligentContentAnalyzer

# Uses configured DSPy provider (Mistral if set)
analyzer = IntelligentContentAnalyzer()
analysis = analyzer.analyze_content(content_text)
```

## Error Handling

### Connection Issues

```python
from common_tools.mistral_dspy_provider import MistralDSPyProvider

try:
    provider = MistralDSPyProvider(api_key=api_key)
    if not provider.test_connection():
        print("❌ Mistral connection failed")
        # Fallback to other provider
except Exception as e:
    print(f"❌ Mistral setup error: {e}")
```

### API Key Validation

```python
from common_tools.dspy_config_manager import get_dspy_status

status = get_dspy_status()
if not status['api_keys_present']['mistral']:
    print("⚠️ Mistral API key not found")
    print("Set MISTRAL_API_KEY environment variable")
```

## Testing

### Test Mistral Integration

```bash
cd agents
python -m common_tools.mistral_dspy_provider
```

### Test DSPy Configuration

```bash
cd agents
python -m common_tools.dspy_config_manager
```

## Troubleshooting

### Common Issues

1. **"Mistral not available"**
   - Install mistralai package: `uv add mistralai`
   - Check import errors in logs

2. **"API key not found"**
   - Set `MISTRAL_API_KEY` environment variable
   - Verify key is valid and has sufficient credits

3. **"Connection test failed"**
   - Check internet connectivity
   - Verify API key permissions
   - Check Mistral service status

### Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

from common_tools.dspy_config_manager import configure_dspy_from_config
configure_dspy_from_config(force_provider="mistral")
```

## Migration from Gemini/OpenAI

### Step-by-Step Migration

1. **Install Mistral dependencies**
2. **Set environment variables**
3. **Update config.yaml**
4. **Test configuration**
5. **Deploy with fallback enabled**

### Gradual Rollout

```yaml
# Enable feature flag for gradual migration
feature_flags:
  mistral_dspy_enabled: true
  mistral_primary_provider: false  # Keep as fallback initially
```

## Performance Optimization

### Caching

The DSPy boot optimization system automatically caches optimized modules:

```bash
# Set cache directory
export DSPY_CACHE_DIR="/opt/dspy_cache"

# Run optimization
python scripts/dspy_boot_optimizer.py --cache-dir /opt/dspy_cache
```

### Batch Processing

For high-volume processing, consider using `mistral-small-latest`:

```python
# Override model for batch jobs
provider = MistralDSPyProvider(
    model="mistral-small-latest",
    api_key=api_key,
    max_tokens=1024  # Smaller responses for faster processing
)
```

## Security Considerations

### API Key Management

- Store API keys in environment variables or secure vaults
- Never commit API keys to code repositories
- Use different keys for development/production
- Monitor API usage and set spending limits

### Rate Limiting

Mistral has rate limits. The system includes automatic retry logic:

```python
# Configured in common_tools/retry_framework.py
# Automatic backoff for rate limit errors
```

## Support and Resources

- [Mistral AI Documentation](https://docs.mistral.ai/)
- [DSPy Documentation](https://dspy-docs.vercel.app/)
- [Umbrix CTI Platform Docs](../docs/)

## Version Compatibility

- **Mistral AI Client**: >= 0.4.0
- **DSPy**: >= 2.4.0  
- **Python**: >= 3.8

## Contributing

To extend Mistral integration:

1. Update `mistral_dspy_provider.py` for new features
2. Add tests in `tests/test_mistral_integration.py`
3. Update documentation
4. Test with existing DSPy modules