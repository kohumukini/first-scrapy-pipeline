import subprocess
import sys

from pathlib import Path
from prefect import flow, task

base_path = Path(__file__).parent.parent

@task 
def run_scrapy(): 
    scrapy_dir = base_path / "scraping" / "hn_scraper"

    print(f"Targeting Directory: {scrapy_dir}")

    subprocess.run(
        ["scrapy", "crawl", "topstories", "-O", "hn_raw.json"], 
        check = True, 
        cwd=str(scrapy_dir))

@task 
def load_raw(): 
    etl_script = base_path / "etl" / "load_raw.py"

    subprocess.run([sys.executable, str(etl_script), "etl/load_raw.py"], check = True, cwd=str(base_path))

@task
def transform_clean(): 
    subprocess.run([sys.executable, "etl/transform_clean.py"], check = True)

@task
def build_analysis(): 
    subprocess.run([sys.executable, "etl/build_analysis.py"], check = True)

@flow(name = "Hacker News Pipeline")
def hn_pipeline(): 
    run_scrapy()
    load_raw()
    transform_clean()
    build_analysis()

if __name__ == "__main__": 
    try: 
        hn_pipeline()
    except Exception as e: 
        print(f"Pipeline Burst: {e}")