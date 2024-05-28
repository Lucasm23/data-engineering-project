<h1>Creation of a data pipeline to perform extraction, loading, and querying of the Latin America GDP (Gross Domestic Product) API: 1</h1>

Required Libraries:

<ol>
    <li>requests</li>
    <li>psycopg2</li>
    <li>dotenv</li>
</ol>
    


<h3>Methods for API Extraction:</h3>

`def fetch_page_data(page):
    url = f"https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json&page={page}&per_page=50"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data. HTTP Status code: {response.status_code}")`

This function uses the requests library to validate the API request. If it fails to extract the content, a message will appear: "Failed to fetch data. HTTP Status code" followed by the error code.
It's important to remember that this method only fetches one page at a time, so storing these pages is necessary:

`data = fetch_page_data(1)
metadata = data[0]
total_pages = metadata['pages']`

Next, we loop through the remaining pages and print the total size of the list with all the data:

`for page in range(2, total_pages + 1):
    data = fetch_page_data(page)
    for record in data[1]:
        entry = (
            record["country"]["id"],
            record["country"]["value"],
            record["date"],
            record["value"]
        )
        all_records.append(entry)`

`print(f"Total records fetched: {len(all_records)}")`

At this point, we create two lists to later construct the tables for countries and GDP:

`countries = list({(record[0], record[1], record[0]) for record in all_records}) 
gdp_records = [(record[0], record[2], record[3]) for record in all_records]`

Next, we set the environment variable parameters to create the new database:

`db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')`

Here, the psycopg2 library is necessary to connect to PostgreSQL:

`conn = psycopg2.connect(
    dbname='postgres',
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
conn.autocommit = True
cur = conn.cursor()`

After creating the connection, it's time to create the new database and connect to it:

`create_db_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
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
cur = conn.cursor()`

Next, we create the tables to perform the query:

`create_country_table_query = """
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
"""`

`cur.execute(create_country_table_query)
cur.execute(create_gdp_table_query)`

Continuing with the insertion of values into the two tables using the two lists created earlier:

`insert_country_query = "INSERT INTO country (id, name, iso3_code) VALUES %s ON CONFLICT (id) DO NOTHING"  # Updating query
insert_gdp_query = "INSERT INTO gdp (country_id, year, value) VALUES %s"`

`execute_values(cur, insert_country_query, countries)
execute_values(cur, insert_gdp_query, gdp_records)`

`conn.commit()`

Finally, we have the pivot_query to filter and return the table with ID, name, Iso3code, and the subsequent years. (The result could be divided to return in billions, but for readability, it was unnecessarily confusing. I chose to leave it as is for faster and more concise reading):
  
`pivot_query = """
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
"""`
`cur.execute(pivot_query)
report = cur.fetchall()
for row in report:
    print(row)`

`cur.close()
conn.close()`

Lastly, a print statement ensures that the code ran effectively:

print("Data has been successfully inserted into the PostgresSQL database and report generated")

![image](https://github.com/Lucasm23/data-engineering-project/assets/83221259/7b168b92-0907-4bbb-aff4-2dc2154dcac8)

![image](https://github.com/Lucasm23/data-engineering-project/assets/83221259/e926bc2f-b754-4ca7-9070-83547d3174f2)

--------------------------------------------------------------------------------------------------------------------------

At this point the script it's working correctly as intended, now we can focus on buildiing the project using docker-compose

In this case, we dont need more than two services, one for postgres and the other to run the script:

`version: '3.8'
services:
  postgres:
    image: postgres:latest
    container_name: postgres-db
    environment:
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: ${DB_NAME}
    ports:
      - "5432:5432"
    networks:
      - my-network
  your-service:
    build: .
    env_file:
      - .env
    depends_on:
      - postgres
    networks:
      - my-network
networks:
  my-network:`


To fully connect, docker compose in this case your host env should be postgres and not localhost.

![image](https://github.com/Lucasm23/data-engineering-project/assets/83221259/bc6d0dbb-8f88-45c7-8df6-1cfe91f61846)



