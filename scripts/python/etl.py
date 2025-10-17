import os
from dotenv import load_dotenv

import pandas as pd
import hashlib
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm

load_dotenv()

DBNAME = os.getenv("DBNAME")
USER = os.getenv("DBUSER")
PASSWORD = os.getenv("DBPASSWORD")
HOST = os.getenv("DBHOST")
PORT = os.getenv("DBPORT")


def hashkey(*args):
    return hashlib.md5("|".join([str(a) for a in args]).encode()).hexdigest()


df = pd.read_csv("data/SampleSuperstore.csv")

conn = psycopg2.connect(
    f"""
    host={HOST}
    port={PORT}
    dbname={DBNAME}
    user={USER}
    password={PASSWORD}
    target_session_attrs=read-write
    sslmode=verify-full
"""
)
cur = conn.cursor()

# 0. DDL

print("Creating tables...")
for query in tqdm(os.listdir("scripts/sql")):
    if query.endswith(".sql"):
        with open(os.path.join("scripts/sql", query), "r") as f:
            cur.execute(f.read())
        conn.commit()
print("Tables created.")

# 1. Хабы
stores = df[["Country", "City", "State", "Postal Code", "Region"]].drop_duplicates()
products = df[["Category", "Sub-Category"]].drop_duplicates()
shipmodes = df[["Ship Mode"]].drop_duplicates()
segments = df[["Segment"]].drop_duplicates()

store_records = [
    (
        hashkey(
            row["Country"], row["City"], row["State"], row["Postal Code"], row["Region"]
        ),
        f"{row['Country']}|{row['City']}|{row['State']}|{row['Postal Code']}|{row['Region']}",
        pd.Timestamp.now(),
        "csv_load",
    )
    for _, row in stores.iterrows()
]
print("Inserting hub records...")
execute_batch(
    cur,
    "INSERT INTO hub_store (store_hashkey, store_business_key, load_dts, record_source) VALUES (%s, %s, %s, %s)",
    store_records,
)
print("Hub records inserted.")

product_records = [
    (
        hashkey(row["Category"], row["Sub-Category"]),
        f"{row['Category']}|{row['Sub-Category']}",
        pd.Timestamp.now(),
        "csv_load",
    )
    for _, row in products.iterrows()
]
print("Inserting hub products...")
execute_batch(
    cur,
    "INSERT INTO hub_product (product_hashkey, product_business_key, load_dts, record_source) VALUES (%s, %s, %s, %s)",
    product_records,
)
print("Hub products inserted.")

shipmode_records = [
    (hashkey(row["Ship Mode"]), row["Ship Mode"], pd.Timestamp.now(), "csv_load")
    for _, row in shipmodes.iterrows()
]
execute_batch(
    cur,
    "INSERT INTO hub_shipmode (shipmode_hashkey, shipmode_business_key, load_dts, record_source) VALUES (%s, %s, %s, %s)",
    shipmode_records,
)

segment_records = [
    (hashkey(row["Segment"]), row["Segment"], pd.Timestamp.now(), "csv_load")
    for _, row in segments.iterrows()
]
print("Inserting hub segments...")
execute_batch(
    cur,
    "INSERT INTO hub_segment (segment_hashkey, segment_business_key, load_dts, record_source) VALUES (%s, %s, %s, %s)",
    segment_records,
)
print("Hub segments inserted.")

# 2. Линк и сателлиты
sale_records = []
sat_sale_records = []
sat_store_records = []
sat_product_records = []

print("Inserting link records...")
for idx, row in tqdm(df.iterrows()):
    sale_hashkey = hashkey(
        row["Ship Mode"],
        row["Segment"],
        row["Country"],
        row["City"],
        row["State"],
        row["Postal Code"],
        row["Region"],
        row["Category"],
        row["Sub-Category"],
        row["Sales"],
        row["Quantity"],
        row["Discount"],
        row["Profit"],
        idx,
    )
    store_hashkey = hashkey(
        row["Country"], row["City"], row["State"], row["Postal Code"], row["Region"]
    )
    product_hashkey = hashkey(row["Category"], row["Sub-Category"])
    shipmode_hashkey = hashkey(row["Ship Mode"])
    segment_hashkey = hashkey(row["Segment"])

    sale_records.append((sale_hashkey, idx, pd.Timestamp.now(), "csv_load"))
    sat_sale_records.append(
        (
            sale_hashkey,
            row["Sales"],
            row["Quantity"],
            row["Discount"],
            row["Profit"],
            pd.Timestamp.now(),
            "csv_load",
        )
    )
    sat_store_records.append(
        (
            store_hashkey,
            row["Country"],
            row["City"],
            row["State"],
            row["Postal Code"],
            row["Region"],
            pd.Timestamp.now(),
            "csv_load",
        )
    )
    sat_product_records.append(
        (
            product_hashkey,
            row["Category"],
            row["Sub-Category"],
            pd.Timestamp.now(),
            "csv_load",
        )
    )

    cur.execute(
        "INSERT INTO link_sale (link_sale_hashkey, sale_hashkey, store_hashkey, product_hashkey, shipmode_hashkey, segment_hashkey, load_dts, record_source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            hashkey(
                sale_hashkey,
                store_hashkey,
                product_hashkey,
                shipmode_hashkey,
                segment_hashkey,
            ),
            sale_hashkey,
            store_hashkey,
            product_hashkey,
            shipmode_hashkey,
            segment_hashkey,
            pd.Timestamp.now(),
            "csv_load",
        ),
    )
print("Link records inserted.")

print("Inserting satellite records...")

print("Inserting hub sale records...")
execute_batch(
    cur,
    "INSERT INTO hub_sale (sale_hashkey, sale_business_key, load_dts, record_source) VALUES (%s, %s, %s, %s)",
    sale_records,
)
print("Hub sale records inserted.")

print("Inserting satellite sale records...")
execute_batch(
    cur,
    "INSERT INTO sat_sale (sale_hashkey, sales, quantity, discount, profit, load_dts, record_source) VALUES (%s, %s, %s, %s, %s, %s, %s)",
    sat_sale_records,
)
print("Satellite sale records inserted.")

print("Inserting satellite store records...")
execute_batch(
    cur,
    "INSERT INTO sat_store (store_hashkey, country, city, state, postal_code, region, load_dts, record_source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
    sat_store_records,
)
print("Satellite store records inserted.")

print("Inserting satellite product records...")
execute_batch(
    cur,
    "INSERT INTO sat_product (product_hashkey, category, sub_category, load_dts, record_source) VALUES (%s, %s, %s, %s, %s)",
    sat_product_records,
)
print("Satellite product records inserted.")

conn.commit()
cur.close()
conn.close()
