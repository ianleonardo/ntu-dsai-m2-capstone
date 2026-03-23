# Orchestration Guideline (Dagster)

This document provides instructions on how to orchestrate the entire data pipeline using Dagster, including integration with the dashboard UI.

## Overview

Dagster serves as the orchestration layer, connecting ingestion (Meltano) and transformation (dbt). It manages dependencies, scheduling, and error handling while providing visibility into pipeline execution and data freshness.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SEC Data      │───▶│   Meltano       │───▶│   BigQuery      │
│   Sources       │    │   Ingestion     │    │   Staging       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Dashboard UI  │◀───│   Dagster       │◀───│   dbt Models    │
│   (FastAPI)     │    │   Orchestration │    │   (Transform)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Configuration

The Dagster project is located in `dataprocessing/dagster_orchestration`.

- `repository.py`: Main entry point defining assets, jobs, and schedules
- `assets/`: Definitions for individual data assets (Meltano jobs, dbt models, custom logic)
- `jobs/`: Pipeline job definitions with dependencies
- `schedules/`: Automated execution schedules

## Asset-Based Architecture

### Core Assets

1. **Data Ingestion Assets**
   - `sec_direct_ingestion`: Complete SEC data download and BigQuery loading
   - `sp500_stock_daily_staging_data`: Market data ingestion from Yahoo Finance

2. **Transformation Assets**
   - `dbt_insider_transformation`: Full dbt model execution with testing
   - `bigquery_sp500_stock_daily_data`: Market data processing and modeling

3. **Summary Assets**
   - `sec_pipeline_summary`: Pipeline execution metadata and statistics
   - `sp500_stock_daily_pipeline_summary`: Market data pipeline metrics

### Asset Dependencies

```
sec_direct_ingestion → dbt_insider_transformation → Dashboard API Updates
sp500_stock_daily_staging_data → bigquery_sp500_stock_daily_data → Dashboard Refresh
```

## Running Dagster

### 1. Using Dagster UI (Highly Recommended)

To launch the web-based environment for development and monitoring:

```bash
uv run dagster dev
```

The UI will typically be available at `http://localhost:3000`.

From the UI, you can:
- **Visualize the asset graph**: See dependencies between pipeline components
- **Launch manual runs**: Execute specific jobs or the entire pipeline
- **Monitor execution**: Real-time logs and task status
- **Manage schedules**: Configure and enable automated runs
- **View materializations**: Track data freshness and quality metrics

### 2. Using Dagster CLI

To list assets in the repository:
```bash
uv run dagster asset list -m dataprocessing.dagster_orchestration.repository
```

To execute a specific job:
```bash
uv run dagster job execute -m dataprocessing.dagster_orchestration.repository -j sec_pipeline_direct_complete_job
```

To materialize specific assets:
```bash
uv run dagster asset materialize -m dataprocessing.dagster_orchestration.repository --select sec_direct_ingestion
```

## Available Jobs

### Complete Pipeline Jobs

**`sec_pipeline_direct_complete_job`**
The full end-to-end pipeline for SEC data:
1. SEC data download from EDGAR
2. Data loading to BigQuery staging tables
3. dbt transformation execution
4. Data quality testing
5. Pipeline summary generation

**`sp500_stock_daily_pipeline_job`**
Complete market data pipeline:
1. S&P 500 company data sync
2. Daily stock price ingestion
3. Market data transformation
4. Integration with insider transaction data

### Specialized Jobs

**`sec_direct_ingestion_job`**: Only the ingestion phase
- Downloads SEC data for specified year/quarter
- Loads to BigQuery staging tables
- Handles incremental updates

**`dbt_transformation_job_direct`**: Only the dbt transformation phase
- Runs all dbt models and tests
- Updates production tables
- Generates documentation

**`sec_dedupe_only_job`**: Data deduplication maintenance
- Removes duplicate records from staging tables
- Optimizes BigQuery table performance
- Useful for data cleanup operations

## Automated Scheduling

### Quarterly SEC Schedule

```python
quarterly_sec_schedule:
  Trigger: Start of each quarter (Jan 1, Apr 1, Jul 1, Oct 1)
  Action: Full SEC data refresh for previous quarter
  Dependencies: BigQuery availability, sufficient quota
  Notifications: Email on failure, success summary
```

### Monthly Validation Schedule

```python
monthly_validation_schedule:
  Trigger: 1st of each month at 2:00 AM
  Action: Data quality and completeness checks
  Scope: All production tables and critical metrics
  Notifications: Alert on any quality issues
```

### Weekly Health Check

```python
weekly_health_check_schedule:
  Trigger: Every Sunday at 6:00 AM
  Action: Pipeline connectivity and performance checks
  Metrics: API response times, query performance, storage usage
  Notifications: Performance degradation alerts
```

### Year-End Processing

```python
year_end_schedule:
  Trigger: January 5th annually
  Action: Complete year data processing and archival
  Scope: Full year SEC data, annual aggregations
  Notifications: Processing summary and data quality report
```

## Monitoring and Observability

### Dagster UI Features

**Asset Graph Visualization**
- Real-time dependency mapping
- Asset health indicators
- Materialization status
- Data freshness metrics

**Execution History**
- Detailed run logs and performance metrics
- Error tracking and debugging information
- Resource utilization monitoring
- Cost tracking for BigQuery operations

**Materialization Tracking**
- Last successful run timestamps
- Data volume processed
- Test pass/fail rates
- Quality metrics over time

### Key Performance Indicators

**Pipeline Performance**
- Execution duration trends
- Data processing rates (records/second)
- BigQuery query costs
- Resource utilization

**Data Quality**
- Test pass rates by model
- Data freshness metrics
- Volume anomaly detection
- Completeness percentages

**System Health**
- API response times
- Database connection health
- Storage utilization
- Error rates

## Integration with Dashboard UI

### Real-time Updates

The dashboard UI automatically reflects pipeline updates:

1. **Data Freshness**: Dashboard shows latest data timestamps
2. **Quality Indicators**: Test results displayed in UI
3. **Processing Status**: Real-time pipeline status indicators
4. **Performance Metrics**: Query performance and response times

### API Integration

The FastAPI backend integrates with Dagster through:

```python
# Backend checks for pipeline status
from dagster import build_assets_job

# Dashboard displays pipeline metadata
GET /api/pipeline/status
GET /api/pipeline/last-run
GET /api/data/freshness
```

### Caching Strategy

Dagster materialization triggers dashboard cache updates:

```python
# When assets materialize, clear relevant caches
@asset
def sec_pipeline_summary():
    # Process pipeline data
    result = process_pipeline_data()
    
    # Trigger dashboard cache refresh
    invalidate_dashboard_cache("insider_transactions")
    
    return result
```

## Advanced Configuration

### Custom Sensors

Create custom sensors for reactive pipeline execution:

```python
@sensor(job=sec_direct_ingestion_job)
def sec_data_availability_sensor(context):
    """Trigger pipeline when new SEC data is available"""
    if check_sec_data_availability():
        yield RunRequest(
            run_key=f"sec_data_{datetime.now()}",
            tags={"source": "sensor"}
        )
```

### Partitioned Assets

For large datasets, use time-based partitioning:

```python
@asset(partitions_def=MonthlyPartitionsDefinition(start_date="2023-01-01"))
def monthly_sec_insider_data(context):
    """Process SEC data by month for better performance"""
    partition_date = context.partition_key
    return process_monthly_data(partition_date)
```

### Resource Management

Configure resources for optimal performance:

```python
@resource(config_schema={"max_workers": int, "timeout": int})
def bigquery_resource(context):
    """BigQuery resource with configurable parameters"""
    return BigQueryClient(
        max_workers=context.resource_config["max_workers"],
        timeout=context.resource_config["timeout"]
    )
```

## Troubleshooting

### Common Issues

**Port Conflict**: If port 3000 is taken, use:
```bash
uv run dagster dev -p 3001
```

**Module Not Found**: Ensure you're running commands from the root directory:
```bash
# From project root
uv run dagster dev
# NOT from dataprocessing/dagster_orchestration/
```

**Asset Dependencies**: Check asset graph for circular dependencies:
```bash
uv run dagster asset list --graph
```

**Memory Issues**: For large datasets, increase memory limits:
```bash
export DAGSTER_DAEMON_MEMORY_LIMIT="4GB"
uv run dagster dev
```

### Performance Issues

**Slow Materialization**: Optimize with incremental loading:
```bash
# Use incremental mode for large tables
uv run dagster asset materialize --select sec_direct_ingestion --incremental
```

**BigQuery Costs**: Monitor query costs in the UI:
- Check the "Cost" tab in asset details
- Use query result caching
- Optimize clustering and partitioning

### Debugging Tips

**Asset Debugging**: Use the UI to inspect asset outputs:
- Click on any asset in the graph
- View "Materializations" tab
- Check "Events" for detailed logs

**Run Debugging**: Examine failed runs:
- Go to "Runs" tab
- Click on failed run
- Review step-by-step execution logs

**API Issues**: Test dashboard integration:
```bash
# Test API endpoints
curl http://localhost:8000/api/pipeline/status
curl http://localhost:8000/api/data/freshness
```

## Best Practices

### Development Workflow

1. **Local Development**: Use `dagster dev` for iterative development
2. **Asset Testing**: Test individual assets before full pipeline runs
3. **Incremental Updates**: Use incremental materialization for large datasets
4. **Monitoring**: Set up alerts for pipeline failures and performance issues

### Production Deployment

1. **Scheduling**: Configure appropriate schedules for data freshness
2. **Resource Management**: Set appropriate memory and CPU limits
3. **Monitoring**: Implement comprehensive monitoring and alerting
4. **Backup**: Regular backups of critical configurations and data

### Performance Optimization

1. **Parallel Execution**: Configure parallel asset execution
2. **Caching**: Use appropriate caching strategies
3. **Resource Allocation**: Optimize resource allocation for different workloads
4. **Query Optimization**: Optimize BigQuery queries and table design

## Next Steps

After setting up orchestration:

1. **Configure Schedules**: Set up automated execution schedules
2. **Monitor Performance**: Track pipeline metrics and optimize
3. **Integrate Dashboard**: Ensure dashboard reflects pipeline updates
4. **Set Up Alerts**: Configure notifications for pipeline issues
5. **Document Workflows**: Document custom configurations and procedures
