import psycopg2
import os

from dotenv import load_dotenv

load_dotenv()
DB_CONN = os.getenv("DB_CONN")

def transform_raw(): 
    with psycopg2.connect(DB_CONN) as conn: 
        with conn.cursor() as cursor: 

            cursor.execute("TRUNCATE TABLE hn_clean")

            cursor.execute("SELECT payload FROM raw_hn")
            rows = cursor.fetchall()

            for (payload, ) in rows: 
                score = payload.get("score")

                if isinstance(score, str) and " " in score: 
                    score = int(score.split()[0])
                elif isinstance(score, int): 
                    score = score
                else: 
                    score = None

                cursor.execute("""
                    INSERT INTO hn_clean (story_id, title, url, score, author, age, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (story_id) DO NOTHING    
                """, (
                    payload.get("story_id"), 
                    payload.get("title"),
                    payload.get("url"),
                    score, 
                    payload.get("author"), 
                    payload.get("age"), 
                    payload.get("scraped_at")
                ))
        print(f"Transformation complete. {len(rows)} rows processed.")

if __name__ == "__main__": 
    try: 
        transform_raw()
    except Exception as e: 
        print(f"Transformation failed: {e}")