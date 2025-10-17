import os
from dotenv import load_dotenv

import psycopg2

load_dotenv()

DBNAME = os.getenv("DBNAME")
USER = os.getenv("DBUSER")
PASSWORD = os.getenv("DBPASSWORD")
HOST = os.getenv("DBHOST")
PORT = os.getenv("DBPORT")

auth_query = f"""
    host={HOST}
    port={PORT}
    dbname={DBNAME}
    user={USER}
    password={PASSWORD}
    target_session_attrs=read-write
    sslmode=verify-full
"""

conn = psycopg2.connect(auth_query)

print("success")
