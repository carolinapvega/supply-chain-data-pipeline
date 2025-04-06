# schedules.py

from dagster import schedule
from dagster_project.jobs import clean_pipeline_job

@schedule(cron_schedule="0 7 * * *", job=clean_pipeline_job, execution_timezone="America/Sao_Paulo")
def daily_cleaning_schedule(_context):
    return {}
