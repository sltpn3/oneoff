import psycopg2
import psycopg2.extras


def open_conn():
    conn = psycopg2.connect(database='postgres',
                            user='postgres',
                            password='',
                            host='192.168.150.122',
                            port=5432)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return conn, cursor


def close_conn(conn, cursor):
    cursor.close()
    conn.close()


def insert():
    conn, cursor = open_conn()
    sql = """INSERT INTO books (title, attr)
    VALUES
     (
     'PostgreSQL Cheat Sheet',
     '
    "paperback" => 5,
    "publisher" => "postgresqltutorial.com",
    "language"  => "English",
    "ISBN-13"   => "978-1449370001",
    "weight"    => "1 ounces"'
     )"""

    cursor.execute(sql)
    conn.commit()
    close_conn(conn, cursor)


def select_all():
    conn, cursor = open_conn()
    sql = 'SELECT * FROM books'
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print row
    close_conn(conn, cursor)

def select_attr():
    conn, cursor = open_conn()
    sql = """SELECT attr -> 'ISBN-13' AS isbn FROM books;"""
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print row['isbn']
    close_conn(conn, cursor)

def select_aggregate():
    conn, cursor = open_conn()
    sql = """SELECT title, SUM(CAST(paperback AS INTEGER)) FROM (SELECT title, attr -> 'paperback' AS paperback FROM books) a GROUP BY title"""
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print row
    close_conn(conn, cursor)

# insert()
select_all()
# select_attr()
select_aggregate()
