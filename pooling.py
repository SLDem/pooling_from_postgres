import psycopg2
from psycopg2 import pool

try:
    postgres_pool = psycopg2.pool.SimpleConnectionPool(1,  # minimum connections
                                                       20,  # maximum connections
                                                       user='postgres',
                                                       password='postgres',
                                                       host='localhost',
                                                       port='5432',
                                                       database='postgres_pool')
    if postgres_pool:
        print('Connection pool created successfully.')

    postgres_connection = postgres_pool.getconn()
    if postgres_connection:
        print('Successfully received connection from connection pool.')
        postgres_cursor = postgres_connection.cursor()
        postgres_cursor.execute('select * from users')
        users = postgres_cursor.fetchall()

        for row in users:
            print(row)

        postgres_cursor.close()

        postgres_pool.putconn(postgres_connection)
        print('Put away a postgres connection.')

except (Exception, psycopg2.DatabaseError) as err:
    print('Error while connecting to Postgres', err)

finally:
    if postgres_pool:
        postgres_pool.closeall
    print('Postgresql pool connection pool is closed.')
