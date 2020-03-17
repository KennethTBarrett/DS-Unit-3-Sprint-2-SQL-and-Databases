import sqlite3
import psycopg2

# Connect to sqlite3 database
sl_conn = sqlite3.connect('rpg_db.sqlite3')
sl_curs = sl_conn.cursor()

# Get characters from sqlite3 database
characters = sl_curs.execute('SELECT * FROM ' +
                             'charactercreator_character;').fetchall()

# Credentials for PostgreSQL
dbname = 'SNIP'
user = 'SNIP'
password = 'SNIP'
host = 'SNIP'

# Connect to Postgre database
pg_conn = psycopg2.connect(dbname=dbname, user=user,
                           password=password, host=host)
pg_curs = pg_conn.cursor()

# Creating charactercreator_character table in Postgre database
create_character_table = '''CREATE TABLE charactercreator_character (
character_id SERIAL PRIMARY KEY,
name VARCHAR(30),
level INT,
exp INT,
hp INT,
strength INT,
intelligence INT,
dexterity INT,
wisdom INT
);
'''
pg_curs.execute(create_character_table)

# Add the characters from sqlite3 to Postgre database
for character in characters:
    insert_character = '''
        INSERT INTO charactercreator_character
        (name, level, exp, hp, strength, intelligence, dexterity, wisdom)
        VALUES ''' + str(character[1:]) + ';'
    pg_curs.execute(insert_character)

# Confirming it worked...

count_rows_query = '''SELECT COUNT(*) FROM
charactercreator_character;'''

rows_in_sqlite = sl_curs.execute(count_rows_query).fetchone()[0]

pg_curs.execute('SELECT COUNT(*) ' +
                'FROM charactercreator_character;')
rows_in_postgre = pg_curs.fetchone()[0]

if rows_in_sqlite == rows_in_postgre:
    print('Number of Rows Match! Printing First 10 Rows in Postgre...')
    pg_curs.execute('''SELECT * FROM charactercreator_character
                    LIMIT 10;''')
    first_ten = pg_curs.fetchall()  # Getting the results

# Printing results to make sure successful.
    for i in range(0, 10):
        print(f'Row {i + 1}: {first_ten[i][1:]}')
else:
        print('Number of Rows Mismatched!')

pg_curs.close()  # Close cursor
pg_conn.commit()  # Commit all changes
