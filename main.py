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


create_people_sql = '''
CREATE TABLE IF NOT EXISTS people (
    id integer PRIMARY KEY,
    name text NOT NULL,
    surname text NOT NULL,
    occupation text NOT NULL
    );
    '''
create_task_sql = '''
CREATE TABLE IF NOT EXISTS tasks (
    id integer PRIMARY KEY,
    person_id integer NOT NULL,
    title VARCHAR(250) NOT NULL,
    description TEXT,
    status VARCHAR(15) NOT NULL,
    FOREIGN KEY (person_id) REFERENCES people (id)
    );
    '''

def add_person(conn, person):
    '''
    Add a new person into the people table
    :param conn:
    :param person:
    :return: person id
    '''
    sql = '''INSERT INTO people (name, surname, occupation) VALUES(?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, person)
    conn.commit()
    return cur.lastrowid

def add_task(conn, task):
    '''
    Create a new task into the tasks table
    :param conn:
    :param task:
    :return: task id
    '''
    sql = '''INSERT INTO tasks(person_id, title, description, status) VALUES(?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid

def select_all(conn, table):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table}')
    rows = cur.fetchall()
    return rows

def select_where(conn, table, **query):
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f'{k}=?')
        values += (v,)
    q = ' AND '.join(qs)
    cur.execute(f'SELECT * FROM {table} WHERE {q}', values)
    rows = cur.fetchall()
    return rows

def update(conn, table, id, **kwargs):
    parameters = [f'{k}=?' for k in kwargs]
    parameters = ','.join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f'''UPDATE {table} SET {parameters} WHERE id=?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print('OK')
    except sqlite3.OperationalError as e:
        print(e)

def delete_where(conn, table, **kwargs):
    qs= []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f'{k}=?')
        values += (v,)
    q = ' AND '.join(qs)

    sql = f'DELETE FROM {table} WHERE {q}'
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print('Deleted')

def delete_all(conn, table):
    sql = f'DELETE FROM {table}'
    cur= conn.cursor()
    cur.execute(sql)
    conn.commit()
    print('Deleted')


if __name__ == '__main__':

    conn = create_connection('database.db')
    execute_sql(conn, create_people_sql)
    execute_sql(conn, create_task_sql)

    person = ('Krzysiu', 'Jarzyna', 'Szef wsyzstkich szefów')
    pr_id = add_person(conn, person)

    task = (
        pr_id,
        'Przyjść do roboty',
        '8 godzin i do domu',
        'started'
    )

    task_id = add_task(conn, task)

    print(pr_id, task_id)
    conn.commit()
    conn.close()





