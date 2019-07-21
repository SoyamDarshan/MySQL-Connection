from database_connection import Database
import Filter_Data


database_connect = Database()
database_connect.show_databses()
default_database_name = 'emp'
default_table_name = 'emp_details'
default_file_name = 'sample_data.txt'

print('''\n Please press enter if you want to use the default values 
    default_database_name = 'emp'
    default_table_name = 'emp_details'
    default_file_name = 'sample_data.txt'
    ''')

database_name = input("\nPlease provide a database_name\n").strip()
table_name = input("\nPlease provide a table_name\n").strip()
file_name = input("\nPlease Provide a file name\n").strip()

if database_name is "":
    database_name = default_database_name
if table_name is "":
    table_name = default_table_name
if file_name is "":
    file_name = default_file_name

column_names = Filter_Data.get_column_name_from_file(file_name)
data = Filter_Data.get_records_from_file(file_name)

database_connect.create_database(database_name)
database_connect.create_table(database_name, table_name, column_names)
database_connect.insert_or_update_values(
    database_name, table_name, column_names, data)
database_connect.show_records_from_table(
    database_name, table_name)

# Close database connection
database_connect.close_server()
