"""
Dagster schedules for automated SEC data pipeline execution.
"""

from datetime import datetime, timedelta
from typing import List, Optional

from dagster import ScheduleDefinition, RunRequest, DefaultScheduleStatus, build_schedule_context

from ..jobs.sec_pipeline_direct import (
    dbt_transformation_job_direct,
    sec_pipeline_direct_complete_job,
)


def get_current_quarter() -> str:
    """Get the current quarter as q1, q2, q3, or q4."""
    month = datetime.now().month
    if month <= 3:
        return "q1"
    elif month <= 6:
        return "q2"
    elif month <= 9:
        return "q3"
    else:
        return "q4"


def get_previous_quarter() -> tuple[int, str]:
    """Get the previous quarter year and quarter."""
    now = datetime.now()
    year = now.year
    month = now.month
    
    if month <= 3:
        return year - 1, "q4"
    elif month <= 6:
        return year, "q1"
    elif month <= 9:
        return year, "q2"
    else:
        return year, "q3"


def get_quarter_start_date(quarter: str, year: int) -> datetime:
    """Get the start date for a given quarter and year."""
    quarter_start_months = {
        "q1": 1,
        "q2": 4,
        "q3": 7,
        "q4": 10,
    }
    
    month = quarter_start_months[quarter]
    return datetime(year, month, 1)


# Schedule for quarterly SEC data loading
quarterly_sec_schedule = ScheduleDefinition(
    name="quarterly_sec_schedule",
    cron_schedule="0 2 1 */3 *",  # At 2:00 AM on the first day of every quarter (Jan, Apr, Jul, Oct)
    job=sec_pipeline_direct_complete_job,
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs full SEC direct pipeline (ingestion + dbt + summary) for the previous quarter",
    execution_timezone="UTC",
)


def quarterly_sec_schedule_context():
    """Generate run config for quarterly SEC schedule."""
    prev_year, prev_quarter = get_previous_quarter()
    
    return RunRequest(
        run_key=f"quarterly_sec_{prev_year}_{prev_quarter}",
        run_config={
            "ops": {
                "sec_direct_ingestion": {
                    "config": {"year": prev_year, "quarters": [prev_quarter]},
                },
                "sec_direct_pipeline_summary": {
                    "config": {"year": prev_year, "quarters": [prev_quarter]},
                },
            }
        },
        tags={"source": "schedule", "type": "quarterly"},
    )


# Monthly validation schedule
monthly_validation_schedule = ScheduleDefinition(
    name="monthly_validation_schedule",
    job=dbt_transformation_job_direct,
    cron_schedule="0 8 1 * *",  # 8 AM on the 1st of every month
    default_status=DefaultScheduleStatus.STOPPED,
    description="Monthly validation of SEC data pipeline and data quality checks",
    execution_timezone="UTC",
)


def monthly_validation_schedule_context():
    """Generate run config for monthly validation schedule."""
    now = datetime.now()
    # Validate the most recent completed quarter
    prev_year, prev_quarter = get_previous_quarter()
    return RunRequest(
        run_key=f"monthly_validation_{now.strftime('%Y_%m')}",
        run_config={
            "ops": {}
        },
        tags={"source": "schedule", "type": "validation"},
    )


# Weekly health check schedule
weekly_health_check_schedule = ScheduleDefinition(
    name="weekly_health_check_schedule",
    job=sec_pipeline_direct_complete_job,
    cron_schedule="0 7 * * 1",  # 7 AM every Monday
    default_status=DefaultScheduleStatus.STOPPED,
    description="Weekly run: full SEC direct pipeline (ingestion + dbt + summary) on a small window",
    execution_timezone="UTC",
)


def weekly_health_check_schedule_context():
    """Generate run config for weekly health check schedule."""
    now = datetime.now()
    
    # Use a small test dataset for health check
    test_year = now.year - 1  # Previous year to ensure data is available
    test_quarter = "q1"  # First quarter is usually most stable
    
    return RunRequest(
        run_key=f"weekly_health_check_{now.strftime('%Y_%m_%d')}",
        run_config={
            "ops": {
                "sec_direct_ingestion": {
                    "config": {"year": test_year, "quarters": [test_quarter]},
                },
                "sec_direct_pipeline_summary": {
                    "config": {"year": test_year, "quarters": [test_quarter]},
                },
            }
        },
        tags={"source": "schedule", "type": "health_check"},
    )


# Year-end complete load schedule
year_end_schedule = ScheduleDefinition(
    name="year_end_schedule",
    job=sec_pipeline_direct_complete_job,
    cron_schedule="0 6 1 1 *",  # 6 AM on January 1st
    default_status=DefaultScheduleStatus.STOPPED,
    description="Year-end: full SEC direct pipeline for all quarters of the previous year",
    execution_timezone="UTC",
)


def year_end_schedule_context():
    """Generate run config for year-end complete load schedule."""
    prev_year = datetime.now().year - 1
    all_quarters = ["q1", "q2", "q3", "q4"]
    
    return RunRequest(
        run_key=f"year_end_complete_{prev_year}",
        run_config={
            "ops": {
                "sec_direct_ingestion": {
                    "config": {"year": prev_year, "quarters": all_quarters},
                },
                "sec_direct_pipeline_summary": {
                    "config": {"year": prev_year, "quarters": all_quarters},
                },
            }
        },
        tags={"source": "schedule", "type": "year_end"},
    )


# Custom schedule for ad-hoc loading
def create_custom_schedule(
    name: str,
    year: int,
    quarters: List[str],
    cron_expression: str,
    description: str = "Custom SEC data loading schedule",
) -> ScheduleDefinition:
    """
    Create a custom schedule for specific SEC data loading requirements.
    
    Args:
        name: Schedule name
        year: Year to load data for
        quarters: List of quarters to load
        cron_expression: Cron expression for scheduling
        description: Schedule description
        
    Returns:
        Configured ScheduleDefinition
    """
    
    def custom_schedule_context():
        return RunRequest(
            run_key=f"{name}_{datetime.now().strftime('%Y_%m_%d_%H_%M')}",
            run_config={
                "ops": {
                    "sec_direct_ingestion": {
                        "config": {"year": year, "quarters": quarters},
                    },
                    "sec_direct_pipeline_summary": {
                        "config": {"year": year, "quarters": quarters},
                    },
                }
            },
            tags={"source": "schedule", "type": "custom"},
        )
    
    return ScheduleDefinition(
        name=name,
        job=sec_pipeline_direct_complete_job,
        cron_schedule=cron_expression,
        default_status=DefaultScheduleStatus.STOPPED,
        description=description,
        execution_timezone="UTC",
    )


# Schedule factory functions
def create_quarterly_schedule_for_year(year: int) -> ScheduleDefinition:
    """Create a quarterly schedule for a specific year."""
    return create_custom_schedule(
        name=f"quarterly_{year}",
        year=year,
        quarters=["q1", "q2", "q3", "q4"],
        cron_expression="0 6 1 1,4,7,10 *",
        description=f"Quarterly SEC data loading for {year}",
    )


def create_backfill_schedule(years: List[int]) -> ScheduleDefinition:
    """Create a one-time backfill schedule for multiple years."""
    
    def backfill_schedule_context():
        run_requests = []
        for year in years:
            for quarter in ["q1", "q2", "q3", "q4"]:
                run_requests.append(
                    RunRequest(
                        run_key=f"backfill_{year}_{quarter}",
                        run_config={
                            "ops": {
                                "sec_direct_ingestion": {
                                    "config": {"year": year, "quarters": [quarter]},
                                },
                                "sec_direct_pipeline_summary": {
                                    "config": {"year": year, "quarters": [quarter]},
                                },
                            }
                        },
                        tags={"source": "schedule", "type": "backfill"},
                    )
                )
        return run_requests
    
    return ScheduleDefinition(
        name="backfill_schedule",
        job=sec_pipeline_direct_complete_job,
        cron_schedule="0 2 1 1 *",  # Run once on January 1st at 2 AM
        default_status=DefaultScheduleStatus.STOPPED,
        description="One-time backfill of historical SEC data",
        execution_timezone="UTC",
    )
