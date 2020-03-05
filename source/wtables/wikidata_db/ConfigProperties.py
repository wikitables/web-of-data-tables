import psycopg2

from wtables.wikidata_db.ConfigDatabase import config

class ConfigProperties(object):
    def __init__(self):
        self.properties=None

    def loadProperties(self):
        try:
            params = config('../database.ini','files')
            print('Recovering properties')
            return params
        except (Exception) as error:
            print(error)


if __name__ == '__main__':
    con=ConfigProperties()
    params=con.loadProperties()
    print(params)