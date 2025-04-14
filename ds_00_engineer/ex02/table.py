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

def create_table(cur, table_name, headers, sample_row):
	types = [find_col_type(value) for value in sample_row]
	columns = ', '.join(f'{header} {type}' for header, type in zip(headers, types))
	query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns});'
	cur.execute(query)

def populate_table(cur, table_name, headers, rows):
	placeholders = ', '.join(['%s'] * len(headers))
	column_names = ', '.join(headers)
	query = f'INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})'
	for row in rows:
		cur.execute(query, row)

def main():
	path_to_folder = './customer'
	data_files = [file for file in os.listdir(path_to_folder) if file.endswith('.csv')]

	if not data_files:
		return
	
	with connect_to_db() as conn:
		with conn.cursor() as cur:
			for data_file in data_files:
				file_path = os.path.join(path_to_folder, data_file)
				table_name = os.path.splitext(data_file)[0]

				with open(file_path, 'r') as f:
					lines = [line.strip() for line in f if line.strip()]
					headers = lines[0].split(',')
					sample_row = lines[1].split(',')
					data_rows = [line.split(',') for line in lines[1:]]

				create_table(cur, table_name, headers, sample_row)
				populate_table(cur, table_name, headers, data_rows)
				conn.commit()

if __name__ == '__main__':
	main()