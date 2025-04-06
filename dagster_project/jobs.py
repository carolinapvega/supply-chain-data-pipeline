# jobs.py

from dagster import job
from dagster_project.ops import run_clean_pipeline_op

@job
def clean_pipeline_job():
    run_clean_pipeline_op()
