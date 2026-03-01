"""
dodo.py — PyDoit build file
Runs:
  1. pull_fred.py
  2. processing_ffiec_data_3.py
"""

DOIT_CONFIG = {"default_tasks": ["pull_fred", "processing_ffiec_data_3"]}


def task_pull_fred():
    """Pull FRED data."""
    return {
        "actions": ["python src/pull_fred.py"],
        "verbosity": 2,
    }


def task_processing_ffiec_data_3():
    """Process FFIEC call report data."""
    return {
        "actions": ["python src/processing_ffiec_data_3.py"],
        "verbosity": 2,
        "task_dep": ["pull_fred"],
    }