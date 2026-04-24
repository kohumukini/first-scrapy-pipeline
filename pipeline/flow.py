from prefect import flow, task
import subprocess

@task 
def run_scrapy(): 
    subprocess.run(["scrapy", "crawl", "topstories", "-O", "hn_raw.json"], check = True)

@task 
def load_raw(): 
    subprocess.run(["python", "etl/load_raw.py"], check = True)

@task
def transform_clean(): 
    subprocess.run(["python", "etl/transform_clean.py"], check = True)

@task
def build_analysis(): 
    subprocess.run(["python", "etl/build_analysis.py"], check = True)

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