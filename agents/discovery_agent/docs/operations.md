# Operational Guide

This document provides operational procedures, monitoring guidance, and troubleshooting information for the Intelligent Crawler Agent in production environments.

## Production Deployment

### System Requirements

#### Minimum Requirements
- **CPU**: 2 cores
- **Memory**: 4GB RAM
- **Storage**: 10GB available space
- **Network**: Stable internet connection with 10Mbps bandwidth

#### Recommended Requirements
- **CPU**: 4+ cores
- **Memory**: 8GB+ RAM
- **Storage**: 50GB+ available space
- **Network**: High-speed internet connection

### Resource Planning

#### Memory Usage
- Base agent: ~500MB
- Content caching: ~1GB per 10,000 cached pages
- LLM responses: ~100MB per 1,000 queries
- Search results: ~50MB per 1,000 searches

#### CPU Usage
- Search orchestration: Low CPU usage
- Content fetching: Medium CPU usage (I/O bound)
- DSPy extraction: High CPU usage during processing
- LLM query generation: Low CPU usage (API bound)

#### Network Usage
- Search API calls: ~1KB per query
- Content fetching: Variable (1KB-1MB per page)
- LLM API calls: ~5KB per query
- Kafka messaging: ~10KB per message

## Monitoring and Alerting

### Key Metrics

#### Agent Health Metrics
```prometheus
# Agent availability
up{job="intelligent-crawler"}

# Process uptime
process_uptime_seconds{job="intelligent-crawler"}

# Memory usage
process_resident_memory_bytes{job="intelligent-crawler"}

# CPU usage
rate(process_cpu_seconds_total{job="intelligent-crawler"}[5m])
```

#### Business Metrics
```prometheus
# Query generation rate
rate(crawler_queries_generated_total[5m])

# Search success rate
rate(crawler_searches_performed_total{status="success"}[5m]) / 
rate(crawler_searches_performed_total[5m])

# Content fetch success rate
rate(crawler_content_fetched_total{status="success"}[5m]) /
rate(crawler_content_fetched_total[5m])

# Processing latency
histogram_quantile(0.95, crawler_processing_duration_seconds)
```

#### Error Metrics
```prometheus
# Error rates by component
rate(crawler_errors_total{component="search"}[5m])
rate(crawler_errors_total{component="content"}[5m])
rate(crawler_errors_total{component="llm"}[5m])

# Rate limit hits
rate(crawler_rate_limited_total[5m])

# Timeout errors
rate(crawler_timeouts_total[5m])
```

### Alerting Rules

#### Critical Alerts
```yaml
# Agent Down
- alert: CrawlerAgentDown
  expr: up{job="intelligent-crawler"} == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Intelligent Crawler Agent is down"

# High Error Rate
- alert: CrawlerHighErrorRate
  expr: rate(crawler_errors_total[5m]) > 0.1
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "High error rate in crawler agent"

# Memory Usage High
- alert: CrawlerHighMemoryUsage
  expr: process_resident_memory_bytes{job="intelligent-crawler"} > 2e9
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Crawler agent using excessive memory"
```

#### Warning Alerts
```yaml
# Rate Limiting
- alert: CrawlerRateLimited
  expr: rate(crawler_rate_limited_total[5m]) > 0.01
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Crawler hitting rate limits frequently"

# Slow Processing
- alert: CrawlerSlowProcessing
  expr: histogram_quantile(0.95, crawler_processing_duration_seconds) > 30
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Crawler processing is slow"
```

### Grafana Dashboards

#### Overview Dashboard
- Agent uptime and health status
- Request rates and success percentages
- Error rates by component
- Processing latencies
- Resource utilization

#### Detailed Metrics Dashboard
- Search provider breakdown
- Content fetch statistics
- LLM query generation metrics
- Cache hit rates
- Kafka message throughput

## Logging

### Log Levels

#### Production Logging
```bash
export LOG_LEVEL=INFO
```

#### Debug Logging
```bash
export LOG_LEVEL=DEBUG
```

### Log Format

The agent uses structured JSON logging:

```json
{
  "timestamp": "2023-12-01T10:00:00Z",
  "level": "INFO",
  "component": "search_orchestrator",
  "correlation_id": "abc123",
  "message": "Search completed successfully",
  "query": "APT29 indicators",
  "provider": "brave",
  "results_count": 15,
  "duration_ms": 1250
}
```

### Log Aggregation

#### Using Loki
```yaml
# promtail configuration
clients:
  - url: http://loki:3100/loki/api/v1/push
    tenant_id: crawler-agent

scrape_configs:
  - job_name: crawler-logs
    static_configs:
      - targets:
          - localhost
        labels:
          job: intelligent-crawler
          __path__: /var/log/crawler/*.log
```

#### Log Retention
- **Info logs**: 30 days
- **Error logs**: 90 days
- **Debug logs**: 7 days (if enabled)

## Maintenance Procedures

### Regular Maintenance

#### Daily Tasks
1. Check agent health status
2. Monitor error rates and alerts
3. Review resource utilization
4. Check rate limit status

#### Weekly Tasks
1. Review performance metrics trends
2. Analyze search query effectiveness
3. Check content extraction quality
4. Update provider quotas if needed

#### Monthly Tasks
1. Review and rotate API keys
2. Analyze cache performance
3. Update configuration based on usage patterns
4. Performance optimization review

### Scaling Procedures

#### Horizontal Scaling

1. **Deploy Additional Instances**
   ```bash
   # Update replica count
   kubectl scale deployment intelligent-crawler --replicas=3
   ```

2. **Load Balancing**
   - Kafka automatically distributes messages across instances
   - Redis provides shared caching
   - No additional load balancer needed

3. **Resource Monitoring**
   - Monitor per-instance metrics
   - Ensure even load distribution
   - Watch for resource contention

#### Vertical Scaling

1. **Memory Scaling**
   ```yaml
   resources:
     requests:
       memory: "512Mi"
     limits:
       memory: "2Gi"
   ```

2. **CPU Scaling**
   ```yaml
   resources:
     requests:
       cpu: "200m"
     limits:
       cpu: "1000m"
   ```

### Configuration Updates

#### Hot Reload (Non-Critical Settings)
Some configuration can be updated without restart:
- Log levels
- Cache settings
- Rate limit adjustments

#### Restart Required (Critical Settings)
- API keys
- Kafka configuration
- Provider endpoints

#### Blue-Green Deployment
For zero-downtime updates:
1. Deploy new version alongside existing
2. Gradually shift traffic
3. Monitor for issues
4. Complete switch or rollback

## Troubleshooting

### Common Issues

#### 1. Agent Won't Start

**Symptoms**: Agent exits immediately or fails health checks

**Causes**:
- Missing required environment variables
- Invalid API keys
- Kafka connectivity issues
- Redis connectivity issues

**Diagnosis**:
```bash
# Check environment variables
env | grep -E "(KAFKA|GEMINI|CTI_API|REDIS)"

# Test Kafka connectivity
kafka-console-consumer --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --topic normalized.intel --timeout-ms 5000

# Test Redis connectivity
redis-cli -u $REDIS_URL ping

# Check agent logs
docker logs intelligent-crawler-agent
```

#### 2. High Memory Usage

**Symptoms**: Memory usage continuously increases, eventual OOM

**Causes**:
- Large content fetches
- Memory leaks in content processing
- Excessive caching

**Diagnosis**:
```bash
# Check memory metrics
curl http://localhost:8080/metrics | grep process_resident_memory

# Review content size limits
echo $CONTENT_MAX_SIZE

# Check cache statistics
redis-cli -u $REDIS_URL info memory
```

**Resolution**:
- Reduce `CONTENT_MAX_SIZE`
- Implement cache TTL
- Restart agent to clear memory

#### 3. Rate Limiting Issues

**Symptoms**: Frequent rate limit errors, search failures

**Causes**:
- Exceeded provider quotas
- Too aggressive search patterns
- Multiple agents using same API keys

**Diagnosis**:
```bash
# Check rate limit metrics
curl http://localhost:8080/metrics | grep rate_limited

# Review provider quotas
# Check provider dashboards for usage
```

**Resolution**:
- Reduce search frequency
- Upgrade provider plans
- Implement better caching
- Use multiple API keys

#### 4. Content Extraction Failures

**Symptoms**: Content fetching succeeds but extraction fails

**Causes**:
- DSPy model issues
- Malformed content
- Network timeouts
- Site blocking

**Diagnosis**:
```bash
# Check extraction error rates
curl http://localhost:8080/metrics | grep extraction_errors

# Review content fetch logs
grep "extraction_failed" /var/log/crawler/crawler.log

# Test manual content fetch
curl -A "Umbrix CTI Crawler 1.0" https://example.com
```

### Performance Optimization

#### Search Optimization
- Monitor query effectiveness
- Adjust search terms based on results
- Use provider-specific features
- Implement query caching

#### Content Processing
- Optimize DSPy model parameters
- Implement content preprocessing
- Use asynchronous processing
- Cache extraction results

#### Resource Optimization
- Monitor and tune garbage collection
- Optimize database queries
- Use connection pooling
- Implement proper timeouts

## Security Operations

### Security Monitoring

#### Access Monitoring
- Monitor API key usage patterns
- Track unusual request patterns
- Log all external API calls
- Monitor failed authentication attempts

#### Data Protection
- Ensure encrypted communication
- Monitor data retention compliance
- Audit data access patterns
- Implement data anonymization

### Incident Response

#### Security Incidents
1. **Isolate affected systems**
2. **Rotate compromised credentials**
3. **Analyze attack vectors**
4. **Implement mitigations**
5. **Document lessons learned**

#### Data Breaches
1. **Stop data processing**
2. **Assess data exposure**
3. **Notify relevant parties**
4. **Implement containment**
5. **Recovery and monitoring**

## Disaster Recovery

### Backup Procedures

#### Configuration Backup
- Environment variables
- Configuration files
- API keys and secrets
- Deployment manifests

#### Data Backup
- Redis cache snapshots
- Agent state information
- Historical metrics data
- Log archives

### Recovery Procedures

#### Service Recovery
1. **Restore configuration**
2. **Deploy agent instances**
3. **Verify connectivity**
4. **Resume processing**
5. **Monitor for issues**

#### Data Recovery
1. **Restore Redis snapshots**
2. **Reload configuration**
3. **Verify data integrity**
4. **Resume normal operations**

## Support and Escalation

### Support Levels

#### Level 1: Operational Issues
- Agent restarts
- Configuration adjustments
- Basic troubleshooting
- Monitoring response

#### Level 2: Technical Issues
- Performance optimization
- Complex troubleshooting
- Integration problems
- Security incidents

#### Level 3: Development Issues
- Code defects
- Architecture changes
- Feature requests
- Emergency patches

### Contact Information

- **Operations Team**: ops@umbrix.com
- **Security Team**: security@umbrix.com
- **Development Team**: dev@umbrix.com
- **Emergency**: [24/7 On-call rotation]