import os
import psycopg2
from datetime import datetime

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def find_col_type(value):
	try:
		datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
		return 'TIMESTAMP'
	except ValueError:
		pass
	try:
		int(value)
		return 'INTEGER'
	except ValueError:
		pass
	try:
		float(value)
		return 'FLOAT'
	except ValueError:
		pass
	if value.lower() in ['true', 'false']:
		return 'BOOLEAN'
	if len(value) == 1:
		return 'CHAR(1)'
	return 'TEXT'

def create_table():
	return

def populate_table():
	return

def main():
	path_to_folder = './customer'
	data_files = [file for file in os.listdir(path_to_folder) if file.endswith('.csv')]

	if not data_files:
		return
	
	with connect_to_db as conn:
		with conn.cursor as cur:
			for data_file in data_files:
				create_table()
				populate_table()
				conn.commit()

if __name__ == '__main__':
	main()