"""
TTB Pipeline Schedules

Schedule definitions for automated TTB pipeline execution.
"""
from dagster import schedule, RunRequest, DefaultScheduleStatus, ScheduleEvaluationContext

from .ttb_jobs import ttb_raw_pipeline, ttb_analytics_pipeline


@schedule(
    job=ttb_raw_pipeline,
    cron_schedule="0 6 * * *",  # 6 AM UTC daily
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily TTB raw data extraction pipeline"
)
def ttb_daily_schedule(context: ScheduleEvaluationContext):
    """
    Daily schedule for TTB raw data extraction.

    Runs at 6 AM UTC daily to extract previous day's data.
    """
    from datetime import datetime, timedelta

    # Process previous day's data
    run_date = context.scheduled_execution_time.date() - timedelta(days=1)
    partition_key = run_date.strftime("%Y-%m-%d")

    return RunRequest(
        run_key=f"ttb_daily_{partition_key}",
        partition_key=partition_key,
        tags={
            "schedule": "daily",
            "pipeline": "raw_extraction",
            "date": partition_key
        }
    )


@schedule(
    job=ttb_analytics_pipeline,
    cron_schedule="0 8 * * *",  # 8 AM UTC daily
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily TTB analytics pipeline"
)
def ttb_analytics_schedule(context: ScheduleEvaluationContext):
    """
    Daily schedule for TTB analytics pipeline.

    Runs at 8 AM UTC daily, after raw data extraction completes.
    """
    from datetime import datetime, timedelta

    # Process previous day's data
    run_date = context.scheduled_execution_time.date() - timedelta(days=1)
    partition_key = run_date.strftime("%Y-%m-%d")

    return RunRequest(
        run_key=f"ttb_analytics_{partition_key}",
        partition_key=partition_key,
        tags={
            "schedule": "daily",
            "pipeline": "analytics",
            "date": partition_key
        }
    )


@schedule(
    job=ttb_raw_pipeline,
    cron_schedule="0 10 * * 0",  # 10 AM UTC on Sundays
    default_status=DefaultScheduleStatus.STOPPED,
    description="Weekly TTB catch-up pipeline"
)
def ttb_weekend_schedule(context: ScheduleEvaluationContext):
    """
    Weekend schedule for TTB catch-up processing.

    Runs on Sundays to catch up on any missed data from the week.
    """
    from datetime import datetime, timedelta

    # Process last 7 days
    end_date = context.scheduled_execution_time.date() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)

    run_requests = []
    current_date = start_date

    while current_date <= end_date:
        partition_key = current_date.strftime("%Y-%m-%d")

        run_requests.append(
            RunRequest(
                run_key=f"ttb_weekend_catchup_{partition_key}",
                partition_key=partition_key,
                tags={
                    "schedule": "weekend_catchup",
                    "pipeline": "raw_extraction",
                    "date": partition_key,
                    "week_start": start_date.strftime("%Y-%m-%d"),
                    "week_end": end_date.strftime("%Y-%m-%d")
                }
            )
        )

        current_date += timedelta(days=1)

    return run_requests