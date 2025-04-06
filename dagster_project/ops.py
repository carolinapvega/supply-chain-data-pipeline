# ops.py

from dagster import op
from dagster_project.cleandata import run_cleaning_pipeline

@op
def run_clean_pipeline_op():
    run_cleaning_pipeline()
