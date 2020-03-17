import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Postgre SQL Credentials
dbname = 'SNIP'
user = 'SNIP'
password = 'SNIP'
host = 'SNIP'
engine = create_engine('SNIP')

# Read in titanic csv, then send to SQL
df = pd.read_csv('titanic.csv')
df.to_sql('titanic', con=engine)

# Connect to PostgreSQL & Instantiate Cursor
pg_conn = psycopg2.connect(dbname=dbname,
                           user=user, password=password, host=host)
pg_curs = pg_conn.cursor()

# A couple exploratory SQL queries
query1 = 'SELECT COUNT(*) from titanic'  # How many total people?

query2 = '''SELECT AVG("Age") from titanic
WHERE "Survived" = 1;'''  # What is the average age of survivors?

query3 = '''SELECT AVG("Age") from titanic
WHERE "Survived" = 0;'''  # And those who didn't survive?

query4 = '''SELECT COUNT(*) from titanic
WHERE "Sex" = 'female' AND
"Survived" = 0;
'''  # How many deceased female passengers?

query5 = '''SELECT COUNT(*) from titanic
WHERE "Sex" = 'female' AND
"Survived" = 1;
'''  # How many female passengers survived?

query6 = '''SELECT COUNT(*) from titanic
WHERE "Sex" = 'male' AND
"Survived" = 0;
'''  # How many deceased male passengers?

query7 = '''SELECT COUNT(*) from titanic
WHERE "Sex" = 'male' AND
"Survived" = 1;
'''  # How many male passengers survived?

# Execute these queries
print('A few statistics about the Titanic:')

pg_curs.execute(query1)
print(f'Number of Passengers: {pg_curs.fetchone()[0]}')

pg_curs.execute(query2)
print(f'Average Ave of Survivor: {round(pg_curs.fetchone()[0], 1)}')

pg_curs.execute(query3)
print(f'Average Ave of Deceased: {round(pg_curs.fetchone()[0], 1)}')

pg_curs.execute(query4)
print(f'Number of Deceased Females: {pg_curs.fetchone()[0]}')

pg_curs.execute(query5)
print(f'Number of Female Survivors: {pg_curs.fetchone()[0]}')

pg_curs.execute(query6)
print(f'Number of Deceased Males: {pg_curs.fetchone()[0]}')

pg_curs.execute(query7)
print(f'Number of Male Survivors: {pg_curs.fetchone()[0]}')
