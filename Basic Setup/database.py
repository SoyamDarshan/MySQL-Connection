from mysql import connector

DB_CONNECTION = connector.connect(
    host='localhost', user='root', password='SoyamDarshan@123')

my_connection = DB_CONNECTION.cursor()
my_connection.execute('Show databases;')

print("Existing Databases: ")
for x in my_connection:
    print(x)
