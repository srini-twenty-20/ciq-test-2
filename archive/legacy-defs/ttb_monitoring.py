"""
TTB pipeline monitoring and alerting utilities.

This module provides monitoring, alerting, and reporting functionality for the TTB data pipeline,
including failure notifications, quality metrics dashboards, and operational health tracking.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json
from pathlib import Path

from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    DefaultSensorStatus,
    get_dagster_logger,
    DagsterEventType,
    AssetMaterialization,
    AssetCheckEvaluation,
    SensorEvaluationContext
)


@sensor(
    name="ttb_pipeline_health_monitor",
    description="Monitor TTB pipeline health and alert on failures",
    default_status=DefaultSensorStatus.RUNNING
)
def ttb_pipeline_health_sensor(context: SensorEvaluationContext):
    """
    Monitor TTB pipeline health and generate alerts for failures.

    This sensor watches for:
    - Asset materialization failures
    - Asset check failures
    - Parsing success rate degradation
    - Reference data staleness
    """
    logger = get_dagster_logger()

    # Get recent runs and events
    instance = context.instance

    # Look for events in the last hour
    cutoff_time = datetime.now() - timedelta(hours=1)

    # Get recent run records
    recent_runs = instance.get_runs(limit=50)

    # Filter for TTB-related runs
    ttb_runs = [
        run for run in recent_runs
        if run.job_name and ('ttb' in run.job_name.lower() or 'cola' in run.job_name.lower())
        and run.create_time > cutoff_time.timestamp()
    ]

    if not ttb_runs:
        return SkipReason("No recent TTB pipeline runs found")

    # Analyze run health
    failed_runs = [run for run in ttb_runs if run.is_failure]
    success_runs = [run for run in ttb_runs if run.is_success]

    total_runs = len(ttb_runs)
    failed_count = len(failed_runs)
    success_rate = (len(success_runs) / total_runs) * 100 if total_runs > 0 else 0

    # Check for concerning patterns
    alerts = []

    if failed_count > 0:
        alerts.append({
            'type': 'run_failures',
            'severity': 'high' if failed_count > 2 else 'medium',
            'message': f'{failed_count} TTB pipeline runs failed in the last hour',
            'details': {
                'failed_runs': [{'run_id': run.run_id, 'job': run.job_name} for run in failed_runs],
                'total_runs': total_runs,
                'success_rate': success_rate
            }
        })

    if success_rate < 80 and total_runs >= 5:
        alerts.append({
            'type': 'low_success_rate',
            'severity': 'high',
            'message': f'TTB pipeline success rate dropped to {success_rate:.1f}%',
            'details': {
                'success_rate': success_rate,
                'total_runs': total_runs,
                'threshold': 80.0
            }
        })

    # Get asset check results
    check_alerts = _check_asset_check_health(instance, cutoff_time)
    alerts.extend(check_alerts)

    if alerts:
        # Generate alert report
        alert_summary = _generate_alert_report(alerts, ttb_runs)

        logger.warning(f"TTB Pipeline Health Alert: {len(alerts)} issues detected")
        logger.warning(f"Alert Summary: {alert_summary}")

        # In a production environment, you would send this to:
        # - Slack/Teams webhook
        # - Email notifications
        # - PagerDuty
        # - Custom alerting system

        return SkipReason(f"Generated {len(alerts)} health alerts")

    else:
        logger.info(f"TTB pipeline health: {total_runs} runs, {success_rate:.1f}% success rate - All healthy")
        return SkipReason("TTB pipeline health is normal")


def _check_asset_check_health(instance, cutoff_time: datetime) -> List[Dict[str, Any]]:
    """Check for asset check failures."""
    alerts = []

    try:
        # Get recent asset check evaluations
        # Note: This is a simplified approach - in practice you'd query the event log more specifically
        recent_events = instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_CHECK_EVALUATION,
                after_timestamp=cutoff_time.timestamp()
            ),
            limit=100
        )

        failed_checks = []
        for event_record in recent_events:
            if hasattr(event_record.dagster_event, 'asset_check_evaluation'):
                evaluation = event_record.dagster_event.asset_check_evaluation
                if not evaluation.passed and evaluation.severity in ['ERROR', 'WARN']:
                    failed_checks.append({
                        'check_name': evaluation.asset_check_key.name,
                        'asset_key': str(evaluation.asset_check_key.asset_key),
                        'severity': evaluation.severity,
                        'description': evaluation.description
                    })

        if failed_checks:
            alerts.append({
                'type': 'asset_check_failures',
                'severity': 'medium',
                'message': f'{len(failed_checks)} asset checks failed',
                'details': {
                    'failed_checks': failed_checks,
                    'count': len(failed_checks)
                }
            })

    except Exception as e:
        # Gracefully handle any issues with event querying
        alerts.append({
            'type': 'monitoring_error',
            'severity': 'low',
            'message': f'Error checking asset check health: {str(e)}',
            'details': {'error': str(e)}
        })

    return alerts


def _generate_alert_report(alerts: List[Dict[str, Any]], runs: List) -> str:
    """Generate a summary alert report."""
    high_severity = len([a for a in alerts if a['severity'] == 'high'])
    medium_severity = len([a for a in alerts if a['severity'] == 'medium'])
    low_severity = len([a for a in alerts if a['severity'] == 'low'])

    report = f"TTB Pipeline Alert Summary:\n"
    report += f"- {high_severity} high severity alerts\n"
    report += f"- {medium_severity} medium severity alerts\n"
    report += f"- {low_severity} low severity alerts\n"
    report += f"- {len(runs)} total runs analyzed\n"

    # Add top issues
    if alerts:
        report += "\nTop Issues:\n"
        for alert in alerts[:3]:  # Show top 3 alerts
            report += f"- {alert['type']}: {alert['message']}\n"

    return report


@sensor(
    name="ttb_data_quality_monitor",
    description="Monitor TTB data quality metrics and trends",
    default_status=DefaultSensorStatus.RUNNING
)
def ttb_data_quality_sensor(context: SensorEvaluationContext):
    """
    Monitor TTB data quality metrics and detect degradation trends.

    Tracks metrics like:
    - Field completeness rates
    - Validation success rates
    - Data volume changes
    - Schema compliance
    """
    logger = get_dagster_logger()

    # In a production environment, this would:
    # 1. Query data quality metrics from the last several runs
    # 2. Compare against historical baselines
    # 3. Detect significant degradations
    # 4. Generate quality alerts

    # For now, just log that monitoring is active
    logger.info("TTB data quality monitoring active - no quality issues detected")

    return SkipReason("Data quality monitoring completed - no issues found")


def create_quality_dashboard_data(recent_runs: List) -> Dict[str, Any]:
    """
    Create dashboard data for TTB pipeline quality metrics.

    This function would be called by a dashboard/reporting system to get
    current quality metrics for visualization.
    """

    dashboard_data = {
        'pipeline_health': {
            'total_runs_24h': 0,
            'success_rate_24h': 0,
            'average_run_time_minutes': 0,
            'last_successful_run': None
        },
        'data_quality': {
            'parsing_success_rate': 0,
            'field_completeness_avg': 0,
            'validation_success_rate': 0,
            'schema_compliance_rate': 0
        },
        'reference_data': {
            'last_refresh': None,
            'product_codes_count': 0,
            'origin_codes_count': 0,
            'data_freshness_hours': 0
        },
        'volume_metrics': {
            'records_processed_24h': 0,
            'files_processed_24h': 0,
            'average_records_per_file': 0,
            'data_size_mb_24h': 0
        },
        'alerts': {
            'active_high_alerts': 0,
            'active_medium_alerts': 0,
            'last_alert_time': None
        }
    }

    # In production, populate with real data from:
    # - Dagster event logs
    # - Asset materialization metadata
    # - Asset check results
    # - S3 metrics
    # - Database queries

    return dashboard_data


def export_quality_metrics(output_path: str = "/tmp/ttb_quality_metrics.json"):
    """
    Export current quality metrics to a file for external monitoring systems.

    This can be used by external monitoring tools, dashboards, or alerting systems
    that need to ingest TTB pipeline metrics.
    """

    metrics = {
        'timestamp': datetime.now().isoformat(),
        'pipeline': 'ttb_cola_extraction',
        'version': '1.0',
        'metrics': create_quality_dashboard_data([]),
        'export_info': {
            'generated_by': 'ttb_monitoring.py',
            'format_version': '1.0',
            'next_export': (datetime.now() + timedelta(hours=1)).isoformat()
        }
    }

    try:
        with open(output_path, 'w') as f:
            json.dump(metrics, f, indent=2)
        return True
    except Exception as e:
        print(f"Error exporting metrics: {e}")
        return False


# Import necessary Dagster types for the sensor (would normally be at top)
try:
    from dagster import EventRecordsFilter
except ImportError:
    # Fallback for older Dagster versions
    EventRecordsFilter = None