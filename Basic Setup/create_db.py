from database import my_connection as db

DATABASE_NAME = 'emp1'
CREATE_DATABASE = f'CREATE DATABASE if not exists {DATABASE_NAME};'
db.execute(CREATE_DATABASE)
db.execute('Show Databases;')

print(f'CREATED DATABASE {DATABASE_NAME}')
#db.execute(f'USE {DATABASE_NAME};')

print(f"You are connected to - {DATABASE_NAME}")
