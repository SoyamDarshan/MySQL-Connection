import Filter_Data
import create_db
from database import my_connection

#create_db.db.my_connection.execute(f'use {create_db.DATABASE_NAME};')

columns = Filter_Data.get_column_name_from_file('sample_data2.txt')

TABLE_NAME = 'emp_details'
CREATE_TABLE_QUERY = f'create table if not exists {create_db.DATABASE_NAME}.{TABLE_NAME} ('

for i in columns:
    CREATE_TABLE_QUERY += (' '.join([i, 'varchar,']))

CREATE_TABLE_QUERY += ' PRIMARY KEY id);'
my_connection.execute(CREATE_TABLE_QUERY)
