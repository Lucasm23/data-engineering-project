import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import requests


load_dotenv()


def fetch_page_data(page):
    url = f"https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json&page={page}&per_page=50"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data. HTTP Status code: {response.status_code}")


all_records = []


data = fetch_page_data(1)
metadata = data[0]
total_pages = metadata['pages']


for record in data[1]:
    entry = (
        record["country"]["id"],
        record["country"]["value"],
        record["countryiso3code"],  
        record["date"],
        record["value"]
    )
    all_records.append(entry)


for page in range(2, total_pages + 1):
    data = fetch_page_data(page)
    for record in data[1]:
        entry = (
            record["country"]["id"],
            record["country"]["value"],
            record["countryiso3code"],  
            record["date"],
            record["value"]
        )
        all_records.append(entry)

print(f"Total records fetched: {len(all_records)}")


countries = list({(record[0], record[1], record[2]) for record in all_records})  
gdp_records = [(record[0], record[3], record[4]) for record in all_records]


db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')


conn = psycopg2.connect(
    dbname='postgres',
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
conn.autocommit = True
cur = conn.cursor()


create_db_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
try:
    cur.execute(create_db_query)
except psycopg2.errors.DuplicateDatabase:
    print(f"Database {db_name} already exists.")
cur.close()
conn.close()


conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cur = conn.cursor()


create_country_table_query = """
CREATE TABLE IF NOT EXISTS country (
    id TEXT PRIMARY KEY,
    name TEXT,
    iso3_code TEXT  -- Adding iso3_code column
);
"""
create_gdp_table_query = """
CREATE TABLE IF NOT EXISTS gdp (
    country_id TEXT,
    year TEXT,
    value NUMERIC,
    FOREIGN KEY (country_id) REFERENCES country (id)
);
"""
cur.execute(create_country_table_query)
cur.execute(create_gdp_table_query)


insert_country_query = "INSERT INTO country (id, name, iso3_code) VALUES %s ON CONFLICT (id) DO NOTHING"  # Updating query
insert_gdp_query = "INSERT INTO gdp (country_id, year, value) VALUES %s"

execute_values(cur, insert_country_query, countries)
execute_values(cur, insert_gdp_query, gdp_records)

conn.commit()


pivot_query = """
SELECT 
    c.id, c.name, c.iso3_code,  -- Including iso3_code
    MAX(CASE WHEN g.year = '2019' THEN g.value END) AS "2019",
    MAX(CASE WHEN g.year = '2020' THEN g.value END) AS "2020",
    MAX(CASE WHEN g.year = '2021' THEN g.value END) AS "2021",
    MAX(CASE WHEN g.year = '2022' THEN g.value END) AS "2022",
    MAX(CASE WHEN g.year = '2023' THEN g.value END) AS "2023"
FROM 
    country c
JOIN 
    gdp g ON c.id = g.country_id
GROUP BY 
    c.id, c.name, c.iso3_code  -- Including iso3_code
ORDER BY 
    c.id;
"""
cur.execute(pivot_query)
report = cur.fetchall()
for row in report:
    print(row)

cur.close()
conn.close()

print("Data has been successfully inserted into the PostgresSQL database and report generated")
