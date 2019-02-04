import psycopg2

from wtables.wikidata_db.ConfigDatabase import config


class Connection(object):
    def __init__(self):
        self.conn=None

    def connect(self):
        try:
            params = config('../database.ini','postgresql')
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**params)
        except (Exception) as error:
            print(error)

    def close(self):
        if self.conn is not None:
            self.conn.close()

if __name__ == '__main__':
    con=Connection()
