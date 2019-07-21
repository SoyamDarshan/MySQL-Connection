from mysql import connector
from mysql.connector import Error, errorcode
import Filter_Data
import settings


class Database:
    '''
    Create a settings.py and store the password in it.
    '''

    def __init__(self):
        try:
            self.DB_CONNECTION = connector.connect(
                host='localhost',
                user='root',
                password=settings.DATABASE_PASSWORD
            )
            self.my_connection = self.DB_CONNECTION.cursor()
        except Error:
            print("Initialization Failed")
            exit()

    def show_databses(self):
        # List of all available Databases
        self.my_connection.execute('Show databases;')
        print("\nExisting Databases: \n")
        for x in self.my_connection:
            print(x)

    def create_database(self, DATABASE_NAME):
        # Create a database
        CREATE_DATABASE_QUERY = f'CREATE DATABASE if not exists {DATABASE_NAME};'
        self.my_connection.execute(CREATE_DATABASE_QUERY)
        print(f'\n\nCREATED DATABASE {DATABASE_NAME}\n\n')
        self.my_connection.execute(f'use {DATABASE_NAME};')
        print(f'\n\nINSIDE {DATABASE_NAME} DATABASE\n\n')
        self.DB_CONNECTION.commit()

    def create_table(self, DATABASE_NAME, TABLE_NAME, column_names):
        self.set_current_database(DATABASE_NAME)
        if not self.check_if_table_exists(DATABASE_NAME, TABLE_NAME):
            CREATE_TABLE_QUERY = f'CREATE TABLE if not exists `{TABLE_NAME}` (\n'
            for i in column_names:
                i = "`"+i+"`"
                CREATE_TABLE_QUERY += (' '.join([i,  'varchar(255),\n']))
            # FIRST KEY AS PRIMARY KEY
            PRIMARY_KEYS = f' PRIMARY KEY (`{column_names[0]}`));'

        # TO MAINTAIN EACH RECORD AS UNIQUE WE CAN TAKE ALL THE COLUMNS TOGETHER AS PRIMARY KEY
        # Have to re-create the table with a different name
            # PRIMARY_KEYS = ' PRIMARY KEY (`' + \
            #     '`, `'.join(column_names) + '`));'

            CREATE_TABLE_QUERY += PRIMARY_KEYS
            print(CREATE_TABLE_QUERY)
            self.my_connection.execute(CREATE_TABLE_QUERY)
            self.DB_CONNECTION.commit()

    def show_tables(self, DATABASE_NAME):
        '''
        Displays All the tables available in that database.
        '''
        try:
            self.my_connection.execute(f'use {DATABASE_NAME};')
            print(f'\n\nINSIDE {DATABASE_NAME} DATABASE\n')
            self.my_connection.execute(f'show tables;')
            for x in self.my_connection:
                print(x)
        except:
            print('\n\nDatabase not found\n\n')
            if (input("Press 'Y' if you would like to create one else any other key to exit.\n\n").upper()) == 'Y':
                self.create_database(DATABASE_NAME)
            else:
                exit()

    def insert_or_update_values(self, DATABASE_NAME, TABLE_NAME, column_names, data_list):
        '''
        Inserts or Update the value if the record is already present.
        '''
        try:
            for data in data_list:
                INSERT_QUERY = f'INSERT INTO {DATABASE_NAME}.{TABLE_NAME}'
                UPDATE_QUERY = f' ON DUPLICATE KEY UPDATE '
                length_data = len(data)
                for i in range(0, length_data):
                    UPDATE_QUERY += f'{column_names[i]} = "{data[i]}", '

                data = '", "'.join(data)

                INSERT_QUERY += f'({" ,".join(column_names[:length_data])}) VALUES ("{data}")\n'
                print(INSERT_QUERY +
                      UPDATE_QUERY[:len(UPDATE_QUERY)-2], "\n")

                self.my_connection.execute(
                    INSERT_QUERY + UPDATE_QUERY[:len(UPDATE_QUERY)-2])
            self.DB_CONNECTION.commit()
        except Error:
            print(f"\nError: {Error}\n")

    def show_records_from_table(self, DATABASE_NAME, TABLE_NAME):
        try:
            self.my_connection.execute(
                f'SELECT * from {DATABASE_NAME}.{TABLE_NAME}')
            print(f"\n\nList of values present in the table {TABLE_NAME}\n")
            for i in self.my_connection:
                print(i)
        except Error:
            print("\nPlease check the database name or table name\n")

    def set_current_database(self, DATABASE_NAME):
        try:
            self.my_connection.execute(f'use {DATABASE_NAME};')
            print(f'\n\nINSIDE {DATABASE_NAME} DATABASE\n')
            self.DB_CONNECTION.commit()
        except Error:
            print("\nPlease check the Database name\n")

    def close_server(self):
        print('\nCLOSING THE DATABASE SERVER\n')
        self.DB_CONNECTION.close()

    def check_if_table_exists(self, DATABASE_NAME, TABLE_NAME):
        self.set_current_database(DATABASE_NAME)
        self.my_connection.execute(
            f'SELECT * FROM information_schema.tables WHERE table_name = "{TABLE_NAME}"')
        val = [x for x in self.my_connection]
        if val:
            print(f'\n`{TABLE_NAME}` Table exists\n')
        else:
            print(f'\n`{TABLE_NAME}` not found\n')
        return val


if __name__ == '__main__':

    database_connect = Database()
    database_connect.show_databses()
    database_connect.create_database('Test_DB')
    database_connect.show_tables('Test_DB')

    column_names = Filter_Data.get_column_name_from_file('sample_data.txt')
    data = Filter_Data.get_records_from_file('sample_data.txt')
    database_connect.create_table('Test_DB', 'emp2', column_names)
    database_connect.check_if_table_exists('Test_db', 'emp3')

    database_connect.insert_or_update_values(
        'Test_DB', 'emp2', column_names, data)
    database_connect.show_records_from_table('Test_123', 'emp2')

    column_names = Filter_Data.get_column_name_from_file('sample_data.txt')
    data = Filter_Data.get_records_from_file('sample_data.txt')
    database_connect.create_table('Test_DB', 'emp2_123', column_names)
    database_connect.insert_or_update_values(
        'Test_DB', 'emp2_123', column_names, data)
    database_connect.show_records_from_table('Test_DB', 'emp2_123')
    # FALSE TEST
    column_names = Filter_Data.get_column_name_from_file('testing123.txt')
    data = Filter_Data.get_records_from_file('testing1234.txt')
    database_connect.create_table('Test_DB', 'emp2', column_names)
    database_connect.insert_or_update_values(
        'Test_DB', 'emp2', column_names, data)

    database_connect.close_server()
