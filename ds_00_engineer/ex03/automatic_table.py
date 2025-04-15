import os
import psycopg2
import pandas as pd

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def find_col_types(df):
	type_dict = {
		'int64': 'BIGINT',
		'float64': 'NUMERIC(10, 2)',
		'bool': 'BOOLEAN',
		'datetime64[ns]': 'TIMESTAMPTZ',
		'object': 'TEXT'
	}

	column_types = {}
	for column in df.columns:
		dtype = str(df[column].dtype)
		if dtype == 'object' and df[column].str.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC$').any():
			column_types[column] = 'TIMESTAMPTZ'
		elif dtype == 'object' and df[column].str.match(r'^[0-9a-fA-F-]{36}$').any():
			column_types[column] = 'UUID'
		elif dtype in type_dict:
			column_types[column] = type_dict[dtype]
		else:
			column_types[column] = 'TEXT'
	return column_types

def create_table(file_path, table_name, cur):
	with open(file_path, 'r') as f:
		headers = f.readline().strip().split(',')
		first_row = f.readline().strip().split(',')

	df = pd.DataFrame([first_row], columns=headers)
	column_types = find_col_types(df)
	columns = ', '.join(f'{col} {col_type}' for col, col_type in column_types.items())
	query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns});'
	cur.execute(query)

def main():
	path_to_folder = './customer'
	files = [file for file in os.listdir(path_to_folder) if file.endswith('.csv')]

	try:
		with connect_to_db() as conn:
			with conn.cursor() as cur:
				for file in files:
					file_path = os.path.join(path_to_folder, file)
					table_name = file.split('.')[0]
					create_table(file_path, table_name, cur)
					conn.commit()
					print(f"Table successfully created: {table_name}")
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()