import sqlite3
import pandas as pd

conn = sqlite3.connect('buddymove_holidayiq.sqlite3')  # Making New Connection to New DB
c = conn.cursor()

df = pd.read_csv('buddymove_holidayiq.csv')  # Read in our CSV
df.columns = df.columns.str.replace(' ', '_') # Replacing the spaces with Underscores
df.to_sql('review', con=conn)  # Send it to SQL.

# Confirming Row Count = 249
check_row_count_query = "SELECT COUNT(*) FROM review;"
row_count = c.execute(check_row_count_query).fetchone()

if row_count[0] != 249:
    print(f'Row Count is {row_count[0]} - We expected 249.')
else:
    print(f'Row Count is {row_count[0]}, which is expected.')

# How many people who reviewed nature 100+ also reviewed shopping 100+?
nature_shopping_query = "SELECT COUNT(User_Id) FROM review WHERE Nature >= 100 AND Shopping >= 100;"
nature_shopping = c.execute(nature_shopping_query).fetchone()
print(f'{nature_shopping[0]} people who reviewed Nature 100+ also reviewed Shopping 100+.')
