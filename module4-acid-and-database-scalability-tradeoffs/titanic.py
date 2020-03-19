import pandas as pd
import psycopg2
import pymongo
import sqlite3

# PostgreSQL Credentials
dbname = 'SNIP'
user = 'SNIP'
password = 'SNIP'
host = 'SNIP'

# MongoDB Client
client = pymongo.MongoClient("mongodb+srv://admin:ICallThisSNIPPED" +
                             "@cluster0-hyo9i.mongodb.net/test?" +
                             "retryWrites=true&w=majority")
db = client.test

# Create Connections
pg_conn = psycopg2.connect(dbname=dbname, user=user,
                           password=password, host=host)
sl_conn = sqlite3.connect('titanic.sqlite3')

# Cursor Instantiation
pg_curs = pg_conn.cursor()
sl_curs = sl_conn.cursor()

# Send Titanic CSV to SQL
df = pd.read_csv('titanic.csv')
df.rename(columns={'Siblings/Spouses Aboard':
                   'Siblings_Spouses_Aboard',
                   'Parents/Children Aboard':
                   'Parents_Children_Aboard'},
          inplace=True)
df['Name'] = df['Name'].str.replace("'", '')
df.to_sql('titanic', con=sl_conn)

# First, let's grab all character information.
fetch_char_query = '''SELECT * FROM
                      titanic;'''
sl_rows = sl_curs.execute(fetch_char_query).fetchall()

# Let's also grab the row count for later.
sl_count_query = 'SELECT COUNT(*) FROM titanic;'
sl_count = sl_curs.execute(sl_count_query).fetchone()[0]

# Create New Table in PostgreSQL
new_table_postgre = '''CREATE TABLE titanic (
                       Index SERIAL PRIMARY KEY,
                       Survived INT,
                       Pclass INT,
                       Name VARCHAR(255),
                       Sex VARCHAR(10),
                       Age FLOAT,
                       Siblings_Spouses_Aboard INT,
                       Parents_Children_Aboard INT,
                       Fare FLOAT
                       );'''
pg_curs.execute(new_table_postgre)

# Insert Characters from SQLite into Postgre
for i in range(sl_count):
    insert_pg_query = '''INSERT INTO
                         titanic (Index,
                         Survived, Pclass, Name, Sex,
                         Age, Siblings_Spouses_Aboard,
                         Parents_Children_Aboard, Fare)
                         VALUES ''' + str(sl_rows[i]) + ';'
    pg_curs.execute(insert_pg_query)

# Fetch Postgre Table
fetch_postgre = '''SELECT * FROM
                   titanic;'''
pg_curs.execute(fetch_postgre)
postgre_chars = pg_curs.fetchall()

# Grabbing our row count again
pg_curs.execute('SELECT COUNT(*) FROM titanic;')
pg_count = pg_curs.fetchone()[0]

# Send values to MongoDB - Come back to optimize
for i in range(pg_count):
    mongo_list = ({'Survived': str(postgre_chars[i][1]),
                   'Pclass': str(postgre_chars[i][2]),
                   'Name': str(postgre_chars[i][3]),
                   'Sex': str(postgre_chars[i][4]),
                   'Age': str(postgre_chars[i][5]),
                   'Siblings_Spouses_Aboard':
                   str(postgre_chars[i][6]),
                   'Parents_Children_Aboard':
                   str(postgre_chars[i][7]),
                   'Fare': str(postgre_chars[i][8])})
    db.test.insert_one(mongo_list)  # Send to MongoDB

# Let's answer some questions!

# - How many passengers survived, and how many died?
surv_sql = '''SELECT COUNT(*)
              FROM titanic
              GROUP BY Survived;'''
# Fetch Values
surv_count_sl = sl_curs.execute(surv_sql).fetchall()

pg_curs.execute(surv_sql)
surv_count_pg = pg_curs.fetchall()

# SQLite
print('=========== SQLite ===========')
print(f'Titanic Deceased: {surv_count_sl[0][0]}')
print(f'Titanic Survivors: {surv_count_sl[1][0]}')

# PosgreSQL
print('========= PostgreSQL =========')
print(f'Titanic Deceased: {surv_count_pg[0][0]}')
print(f'Titanic Survivors: {surv_count_pg[1][0]}')

# MongoDB
surv_m = db.test.count_documents({'Survived': 1})
died_m = db.test.count_documents({'Survived': 0})

print('=========== MongoDB ===========')
print(f'Titanic Survivors: {surv_m}')
print(f'Titanic Survivors: {died_m}\n')

# - How many passengers were in each class?
pcl_q = '''SELECT COUNT(*)
           FROM titanic
           GROUP BY Pclass
           ORDER BY Pclass;'''
# Fetch Values
sl_pcl = sl_curs.execute(pcl_q).fetchall()

pg_curs.execute(pcl_q)
pg_pcl = pg_curs.fetchall()

# SQLite
print('=========== SQLite ===========')
print(f'Passengers in 1st Class: {sl_pcl[0][0]}')
print(f'Passengers in 2nd Class: {sl_pcl[1][0]}')
print(f'Passengers in 3rd Class: {sl_pcl[2][0]}')

# PostgreSQL
print('========= PostgreSQL =========')
print(f'Passengers in 1st Class: {pg_pcl[0][0]}')
print(f'Passengers in 2nd Class: {pg_pcl[1][0]}')
print(f'Passengers in 3rd Class: {pg_pcl[2][0]}')

# MongoDB
pc1_m = db.test.count_documents({'Pclass': 1})
pc2_m = db.test.count_documents({'Pclass': 2})
pc3_m = db.test.count_documents({'Pclass': 3})

print('=========== MongoDB ===========')
print(f'Passengers in 1st Class: {pc1_m}')
print(f'Passengers in 2nd Class: {pc2_m}')
print(f'Passengers in 3rd Class: {pc3_m}')
print('')  # Line break for readability

# - How many passengers survived/died within each class?
# Deceased
pcl_died = '''SELECT COUNT(*)
              FROM titanic
              WHERE Survived = 0
              GROUP BY Pclass;'''
# Survivors
pcl_surv = '''SELECT COUNT(*)
              FROM titanic
              WHERE Survived = 1
              GROUP BY Pclass;'''
# Fetch Values
pcl_died_sl = sl_curs.execute(pcl_died).fetchall()
pcl_surv_sl = sl_curs.execute(pcl_surv).fetchall()

pg_curs.execute(pcl_died)
pcl_died_pg = pg_curs.fetchall()

pg_curs.execute(pcl_surv)
pcl_surv_pg = pg_curs.fetchall()

# SQLite
print('=========== SQLite ===========')
print(f'Passengers in 1st Class (Deceased): {pcl_died_sl[0][0]}')
print(f'Passengers in 1st Class (Survived): {pcl_surv_sl[0][0]}')
print(f'Passengers in 2nd Class (Deceased): {pcl_died_sl[1][0]}')
print(f'Passengers in 2nd Class (Survived): {pcl_surv_sl[1][0]}')
print(f'Passengers in 3rd Class (Deceased): {pcl_died_sl[2][0]}')
print(f'Passengers in 3rd Class (Survived): {pcl_surv_sl[2][0]}')

# PostgreSQL
print('========= PostgreSQL =========')
print(f'Passengers in 1st Class (Deceased): {pcl_died_pg[0][0]}')
print(f'Passengers in 1st Class (Survived): {pcl_surv_pg[0][0]}')
print(f'Passengers in 2nd Class (Deceased): {pcl_died_pg[1][0]}')
print(f'Passengers in 2nd Class (Survived): {pcl_surv_pg[1][0]}')
print(f'Passengers in 3rd Class (Deceased): {pcl_died_pg[2][0]}')
print(f'Passengers in 3rd Class (Survived): {pcl_surv_pg[2][0]}')

# MongoDB
print('=========== MongoDB ===========')
print('')  # Line break for readability

# - What was the average age of survivors vs nonsurvivors?
avg_age_q = '''SELECT AVG(Age) FROM
               titanic GROUP BY
               Survived;'''
# Fetch Values
avg_age_sl = sl_curs.execute(avg_age_q).fetchall()

pg_curs.execute(avg_age_q)
avg_age_pg = pg_curs.fetchall()

# SQLite
print('=========== SQLite ===========')
print(f'Average Age of Deceased: {avg_age_sl[0][0]}')
print(f'Average Age of Survivor: {avg_age_sl[1][0]}')

# PosgreSQL
print('========= PostgreSQL =========')
print(f'Average Age of Deceased: {avg_age_pg[0][0]}')
print(f'Average Age of Survivor: {avg_age_pg[1][0]}')

# MongoDB - Going to have to manually calculate w/ MongoDB
print('=========== MongoDB ===========')
print('')  # Line break for readability

# - What was the average age of each passenger class?
avg_age_pc = '''SELECT AVG(Age) from
                titanic GROUP BY
                Pclass ORDER BY
                Pclass ASC;'''
# Fetch Values
avg_pc_sl = sl_curs.execute(avg_age_pc).fetchall()

pg_curs.execute(avg_age_pc)
avg_pc_pg = pg_curs.fetchall()

# SQLite
print('=========== SQLite ===========')
print(f'Average Age in 1st Class: {avg_pc_sl[0][0]}')
print(f'Average Age in 2nd Class: {avg_pc_sl[1][0]}')
print(f'Average Age in 3rd Class: {avg_pc_sl[2][0]}')

# PostgreSQL
print('========= PostgreSQL =========')
print(f'Average Age in 1st Class: {avg_pc_pg[0][0]}')
print(f'Average Age in 2nd Class: {avg_pc_pg[1][0]}')
print(f'Average Age in 3rd Class: {avg_pc_pg[2][0]}')

# MongoDB - Going to have to manually calculate w/ MongoDB
print('=========== MongoDB ===========')
print('')  # Line break for readability

# - What was the average fare by passenger class? By survival?
# By Passenger Class
fare_pcl_q = '''SELECT AVG(Fare) FROM
                titanic GROUP BY Pclass
                ORDER BY Pclass ASC;'''
# By Survival
fare_surv_q = '''SELECT AVG(Fare) FROM
                 titanic GROUP BY Survived;'''

# Fetch Values
fare_pcl_sl = sl_curs.execute(fare_pcl_q).fetchall()
pg_curs.execute(fare_pcl_q)
fare_pcl_pg = pg_curs.fetchall()

fare_surv_sl = sl_curs.execute(fare_surv_q).fetchall()
pg_curs.execute(fare_surv_q)
fare_surv_pg = pg_curs.fetchall()

# SQLite
print('=========== SQLite ===========')
print(f'Average Fare in 1st Class: {fare_pcl_sl[0][0]}')
print(f'Average Fare in 2nd Class: {fare_pcl_sl[1][0]}')
print(f'Average Fare in 3rd Class: {fare_pcl_sl[2][0]}')
print(f'Average Fare of Deceased: {fare_surv_sl[0][0]}')
print(f'Average Fare of Survivor: {fare_surv_sl[1][0]}')

# PostgreSQL
print('========= PostgreSQL =========')
print(f'Average Fare in 1st Class: {fare_pcl_pg[0][0]}')
print(f'Average Fare in 2nd Class: {fare_pcl_pg[1][0]}')
print(f'Average Fare in 3rd Class: {fare_pcl_pg[2][0]}')
print(f'Average Fare (Deceased): {fare_surv_pg[0][0]}')
print(f'Average Fare (Survivor): {fare_surv_pg[1][0]}')

# MongoDB - Going to have to manually calculate w/ MongoDB
print('=========== MongoDB ===========')
print('')  # Line break for readability

# - How many siblings/spouses aboard on average, by class? By survival?

# By Class
sib_pcl_q = '''SELECT AVG(
               Siblings_Spouses_Aboard) FROM
               titanic GROUP BY Pclass
               ORDER BY Pclass ASC;'''
# By Survival
sib_surv_q = '''SELECT AVG(
                Siblings_Spouses_Aboard) FROM
                titanic titanic GROUP BY Survived;'''
# Grab info.
sib_pcl_sl = sl_curs.execute(sib_pcl_q).fetchall()
pg_curs.execute(sib_pcl_q)
sib_pcl_pg = pg_curs.fetchall()

sib_surv_sl = sl_curs.execute(sib_surv_q).fetchall()
pg_curs.execute(sib_surv_q)
sib_surv_pg = pg_curs.fetchall()

# SQLite
print('=========== SQLite ===========')
print(f'Average Siblings/Spouses Aboard in 1st Class: {sib_pcl_sl[0][0]}')
print(f'Average Siblings/Spouses Aboard in 2nd Class: {sib_pcl_sl[1][0]}')
print(f'Average Siblings/Spouses Aboard in 3rd Class: {sib_pcl_sl[2][0]}')
print(f'Average Siblings/Spouses Aboard (Deceased): {sib_surv_sl[0][0]}')
print(f'Average Siblings/Spouses Aboard (Survivors): {sib_surv_sl[1][0]}')

# PostgreSQL
print('========= PostgreSQL =========')
print(f'Average Siblings/Spouses Aboard in 1st Class: {sib_pcl_pg[0][0]}')
print(f'Average Siblings/Spouses Aboard in 2nd Class: {sib_pcl_pg[1][0]}')
print(f'Average Siblings/Spouses Aboard in 3rd Class: {sib_pcl_pg[2][0]}')
print(f'Average Siblings/Spouses Aboard (Deceased): {sib_surv_pg[0][0]}')
print(f'Average Siblings/Spouses Aboard (Survivors): {sib_surv_pg[1][0]}')

# MongoDB - Will Have to manually calculate
print('=========== MongoDB ===========')
print('')  # Line break for readability

# - How many parents/children aboard on average, by class? By survival?
# By Class
kid_par_pcl_q = '''SELECT AVG(Parents_Children_Aboard)
                   from titanic
                   GROUP BY Pclass
                   ORDER BY Pclass ASC;'''
# By Survival
kid_par_surv_q = '''SELECT AVG(
                    Parents_Children_Aboard) FROM
                    titanic GROUP BY Survived;'''
# Fetch Values
kid_par_pcl_sl = sl_curs.execute(kid_par_pcl_q).fetchall()
pg_curs.execute(kid_par_pcl_q)
kid_par_pcl_pg = pg_curs.fetchall()

kid_par_surv_sl = sl_curs.execute(kid_par_surv_q).fetchall()
pg_curs.execute(kid_par_surv_q)
kid_par_surv_pg = pg_curs.fetchall()

# SQLite
print('=========== SQLite ===========')
print(f'Average Parents/Children Aboard in 1st Class: {kid_par_pcl_sl[0][0]}')
print(f'Average Parents/Children Aboard in 2nd Class: {kid_par_pcl_sl[1][0]}')
print(f'Average Parents/Children Aboard in 3rd Class: {kid_par_pcl_sl[2][0]}')
print(f'Average Parents/Children Aboard (Deceased): {kid_par_surv_sl[0][0]}')
print(f'Average Parents/Children Aboard (Survivors): {kid_par_surv_sl[1][0]}')

# PostgreSQL
print('========= PostgreSQL =========')
print(f'Average Parents/Children Aboard in 1st Class: {kid_par_pcl_pg[0][0]}')
print(f'Average Parents/Children Aboard in 2nd Class: {kid_par_pcl_pg[1][0]}')
print(f'Average Parents/Children Aboard in 3rd Class: {kid_par_pcl_pg[2][0]}')
print(f'Average Parents/Children Aboard (Deceased): {kid_par_surv_pg[0][0]}')
print(f'Average Parents/Children Aboard (Survivors): {kid_par_surv_pg[1][0]}')

# MongoDB - Will Have to manually calculate
print('=========== MongoDB ===========')
print('')  # Line break for readability

# - Do any passengers have the same name?
same_name_query = '''SELECT COUNT(Name) -
                     COUNT(DISTINCT Name)
                     FROM titanic;'''
# Fetch Values
dup_name_sl = sl_curs.execute(same_name_query).fetchone()

pg_curs.execute(same_name_query)
dup_name_pg = pg_curs.fetchone()

# SQLite
print('=========== SQLite ===========')
print(f'Number of Duplicate Names: {dup_name_sl[0]}')

# SQLite
print('========= PostgreSQL =========')
print(f'Number of Duplicate Names: {dup_name_pg[0]}')

# MongoDB
print('=========== MongoDB ===========')

pg_conn.close()
pg_conn.commit()
