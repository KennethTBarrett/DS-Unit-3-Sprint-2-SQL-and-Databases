import sqlite3
import pandas as pd

# Create connection to new DB, prepare for usage.
conn = sqlite3.connect('buddymove_holidayiq.sqlite3')
c = conn.cursor()

df = pd.read_csv('buddymove_holidayiq.csv')  # Read in our CSV

# Replacing the spaces with Underscores (Easier to work with)
df.columns = df.columns.str.replace(' ', '_')

df.to_sql('review', con=conn)  # Send it to SQL.

# Confirming Row Count = 249
check_row_count_query = "SELECT COUNT(*) FROM review;"
row_count = c.execute(check_row_count_query).fetchone()

if row_count[0] != 249:
    print(f'Row Count is {row_count[0]} - We expected 249.')
else:
    print(f'Row Count is {row_count[0]}, which is expected.')

# How many people who reviewed nature 100+ also reviewed shopping 100+?
ns_query = ("SELECT COUNT(User_Id) FROM review " +
            "WHERE Nature >= 100 AND Shopping >= 100;")
ns = c.execute(ns_query).fetchone()[0]

print(f'{ns} people who reviewed Nature 100+ also reviewed Shopping 100+.')
