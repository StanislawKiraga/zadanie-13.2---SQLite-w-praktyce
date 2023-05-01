import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def execute_sql(conn, sql):
    """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

create_measure_table = '''
CREATE TABLE clean_measure (
    station text PRIMARY KEY,
    date text,
    precip float,
    tobs integer
    );'''

create_stations_table = '''
CREATE TABLE clean_stations (
    station text PRIMARY KEY,
    latitude float,
    longitude float,
    elevation float,
    name text,
    country text,
    state text
    );'''
    
def import_measure_csv():
    conn = create_connection('clean.db')
    cur = conn.cursor()

    with open('/Users/amitrosz-wromac/Downloads/clean_measure.csv', 'r') as file:
        records = 0
        for row in file:
            cur.execute('INSERT OR IGNORE INTO clean_measure VALUES(?,?,?,?)', row.split(','))
            conn.commit()
            records += 1
    conn.close()
    print('OK')

def import_stations_csv():
    conn = create_connection('clean.db')
    cur = conn.cursor()

    with open('/Users/amitrosz-wromac/Downloads/clean_stations.csv', 'r') as file:
        records = 0
        for row in file:
            cur.execute('INSERT OR REPLACE INTO clean_stations VALUES(?,?,?,?,?,?,?)', row.split(','))
            conn.commit()
            records += 1
    conn.close()
    print('OK')

if __name__ == '__main__':
    conn = create_connection('clean.db')
    
    execute_sql(conn, create_measure_table)
    execute_sql(conn, create_stations_table)

    import_measure_csv()
    import_stations_csv()

    result = conn.execute('SELECT * FROM clean_stations LIMIT 5').fetchall()
    for r in result:
        print(r)
    