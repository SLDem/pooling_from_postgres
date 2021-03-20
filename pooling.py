import psycopg2
from psycopg2 import pool
import logging
import time

from contextlib import contextmanager


POOL_DELAY = 0.001


class DBPool(object):
    """
    DB Pool which handles database connections.
    """
    def __init__(self, user, password, db_name, host, port, ttl, pool_size):
        self._connection_pool = []
        self.connection_pointer = 0
        self._pool_size = pool_size
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db_name = db_name
        self.connection_ttl = ttl
        self.log = logging.getLogger('dbpool')

    def __del__(self):
        for connection in self._connection_pool:
            self._close_connection(connection)

    def _create_connection(self):
        """
        Creates connection object.
        :return: dictionary with connection object properties.
        """
        connection = psycopg2.connect(dbname=self._db_name,
                                      user=self._user,
                                      password=self._password,
                                      host=self._host,
                                      port=self._port)
        self.log.info('Created connection object: {}'.format(connection))
        return {'connection': connection,
                'last_update': 0,
                'creation_date': time.time()}

    def _get_connection(self):
        """
        Gets a connection from the pool of calls.
        :return: opened connection psycopg2 object.
        """
        connection = None
        while not connection:
            if self._connection_pool:
                connection = self._connection_pool.pop()
                self.log.info('Popped connection {} from the pool.'.format(connection['connection']))
            elif self.connection_pointer < self._pool_size:
                connection = self._create_connection()
                self.connection_pointer += 1
            time.sleep(POOL_DELAY)
            self.log.info('Waiting for new/free connection. Sleep {}'.format(POOL_DELAY))
        return connection

    @contextmanager
    def manager(self):
        """
        Generator manager manages work with connections.
        :yields: opened connection.
        """
        connection = self._get_connection()
        try:
            yield connection['connection']
        except Exception:
            self._close_connection(connection)
            raise

        if connection['creation_date'] + self.connection_ttl < time.time():
            self._push_connection(connection)
        else:
            self._close_connection(connection)

    @contextmanager
    def transaction(self):
        """
        Manages work with connections for updating the database.
        """
        connection = self._get_connection()
        cursor = connection['connection'].cursor()

        try:
            yield cursor
            connection['connection'].commit()
        except Exception:
            connection['connection'].rollback()
            raise

        if connection['creation_date'] + self.connection_ttl < time.time():
            self._push_connection(connection)
        else:
            self._close_connection(connection)

    def _close_connection(self, connection):
        """
        Closes connection before returning it to the pool.
        :param connection: connection to be closed.
        """
        self.log.info('Closed connection {0} with lifetime of {1}'.format(connection['connection'],
                                                                          connection['creation_date']))
        self.connection_pointer -= 1
        connection['connection'].close()

    def _push_connection(self, connection):
        """
        Pushes connection to pool.
        :param connection: connection to be pushed.
        """
        self.log.info('Returning connection {} to pool'.format(connection['connection']))
        connection['last_update'] = time.time()
        self._connection_pool.append(connection)


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
    
