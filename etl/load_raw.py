import json
import os
import psycopg2

from dotenv import load_dotenv
from psycopg2.extras import Json
from datetime import datetime, UTC

script_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(script_dir, '..', 'scraping', 'hn_scraper', 'hn_raw.json')
final_path = os.path.normpath(json_path)

load_dotenv()

conn = psycopg2.connect(os.getenv("DB_CONN"))

def load_raw(json_path):  
    with psycopg2.connect(os.getenv("DB_CONN")) as conn: 
        with conn.cursor() as cursor: 

            cursor.execute("TRUNCATE TABLE raw_hn")

            with open(json_path) as file_in: 
                data = json.load(file_in)

            rows_to_insert = [
                (datetime.fromisoformat(item["scraped_at"]), Json(item))
                for item in data
            ] 

            from psycopg2.extras import execute_batch
            execute_batch(cursor, """
                INSERT INTO raw_hn (scraped_at, payload)
                VALUES (%s, %s)              
            """, rows_to_insert)   

if __name__ == "__main__": 
    try: 
        load_raw(final_path)
        print("Success: Data loaded!")
    except Exception as e: 
        print(f"Error: {e}")
    finally: 
        conn.close()
    
