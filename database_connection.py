from mysql import connector
import Filter_Data
import settings


class Database:
    def __init__(self):
        self.DB_CONNECTION = connector.connect(
            host='localhost',
            user='root',
            password=settings.DATABASE_PASSWORD
        )
        self.my_connection = self.DB_CONNECTION.cursor()

    def show_databses(self):
        self.my_connection.execute('Show databases;')
        print("Existing Databases: ")
        for x in self.my_connection:
            print(x)

    def create_database(self, DATABASE_NAME):
        CREATE_DATABASE_QUERY = f'CREATE DATABASE if not exists {DATABASE_NAME};'
        self.my_connection.execute(CREATE_DATABASE_QUERY)
        print(f'CREATED DATABASE {DATABASE_NAME}')
        self.my_connection.execute(f'use {DATABASE_NAME};')
        print(f'INSIDE {DATABASE_NAME} DATABASE')

    def create_table(self, DATABASE_NAME, TABLE_NAME, column_names):
        self.set_current_database(DATABASE_NAME)
        if not self.check_if_table_exists(DATABASE_NAME, TABLE_NAME):
            CREATE_TABLE_QUERY = f'CREATE TABLE if not exists `{TABLE_NAME}` (\n'
            for i in column_names:
                i = "`"+i+"`"
                CREATE_TABLE_QUERY += (' '.join([i,  'varchar(255),\n']))
            # CREATE_TABLE_QUERY += ' PRIMARY KEY (`id`));'

        # TO MAINTAIN EACH RECORD AS UNIQUE WE CAN TAKE ALL THE COLUMNS TOGETHER AS PRIMARY KEY
            PRIMARY_KEYS = ' PRIMARY KEY (`' + \
                '`, `'.join(column_names) + '`));'
            CREATE_TABLE_QUERY += PRIMARY_KEYS
            print(CREATE_TABLE_QUERY)
            self.my_connection.execute(CREATE_TABLE_QUERY)

    def show_tables(self, DATABASE_NAME):
        self.my_connection.execute(f'use {DATABASE_NAME};')
        print(f'INSIDE {DATABASE_NAME} DATABASE')
        self.my_connection.execute(f'show tables;')
        for x in self.my_connection:
            print(x)

    def insert_values(self, DATABASE_NAME, TABLE_NAME, column_names, data_list):
        INSERT_QUERY = f'INSERT INTO {DATABASE_NAME}.{TABLE_NAME}'
        for data in data_list:
            length_data = len(data)
            data = '", "'.join(data)
            print(INSERT_QUERY +
                  f'({" ,".join(column_names[:length_data])}) VALUES ("{data}")\n')
            self.my_connection.execute(
                INSERT_QUERY + f'({" ,".join(column_names[:length_data])}) VALUES ("{data}")\n')

    def show_records_from_table(self, DATABASE_NAME, TABLE_NAME):
        self.my_connection.execute(
            f'SELECT * from {DATABASE_NAME}.{TABLE_NAME}')
        for i in self.my_connection:
            print(i)

    def set_current_database(self, DATABASE_NAME):
        self.my_connection.execute(f'use {DATABASE_NAME};')
        print(f'INSIDE {DATABASE_NAME} DATABASE')

    def close_server(self):
        print('CLOSING THE DATABASE SERVER')
        self.DB_CONNECTION.close()

    def check_if_table_exists(self, DATABASE_NAME, TABLE_NAME):
        self.set_current_database(DATABASE_NAME)
        self.my_connection.execute(
            f'SELECT * FROM information_schema.tables WHERE table_name = "{TABLE_NAME}"')
        val = [x for x in self.my_connection]
        if val:
            print(f'`{TABLE_NAME}` Table exists')
        return val


if __name__ == '__main__':

    database_connect = Database()
    database_connect.show_databses()
    # database_connect.create_database('Test_DB')
    database_connect.show_tables('Test_DB')

    column_names = Filter_Data.get_column_name_from_file('sample_data.txt')
    data = Filter_Data.get_records_from_file('sample_data.txt')
    database_connect.create_table('Test_DB', 'emp1', column_names)
    #database_connect.check_if_table_exists('Test_DB', 'emp3')

    database_connect.insert_values('Test_DB', 'emp1', column_names, data)
    database_connect.show_records_from_table('Test_DB', 'emp1')
