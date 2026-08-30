"""Entry point at project root — runs the Kafka producer in jobs/."""
from pathlib import Path
import runpy
import sys

JOBS_DIR = Path(__file__).resolve().parent / "jobs"
sys.path.insert(0, str(JOBS_DIR))
runpy.run_path(str(JOBS_DIR / "main.py"), run_name="__main__")
