from dotenv import load_dotenv
import os
load_dotenv()

print(f"Connecting to: {os.getenv('DB_NAME')}")