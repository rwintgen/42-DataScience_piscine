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
		elif dtype in type_dict:
			column_types[column] = type_dict[dtype]
		else:
			column_types[column] = 'TEXT'
	return column_types

def create_table(cur, table_name, column_types):
	columns = ', '.join(f'{col} {col_type}' for col, col_type in column_types.items())
	query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns});'
	cur.execute(query)

def populate_table(cur, table_name, df):
	placeholders = ', '.join(['%s'] * len(df.columns))
	column_names = ', '.join(df.columns)
	query = f'INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})'
	for row in df.itertuples(index=False, name=None):
		cur.execute(query, row)

def process_csv(file_path, table_name, cur):
	df = pd.read_csv(file_path)
	column_types = find_col_types(df)
	create_table(cur, table_name, column_types)
	populate_table(cur, table_name, df)

def main():
	path_to_folder = './customer'
	files = [file for file in os.listdir(path_to_folder) if file.endswith('.csv')]

	if not files:
		return
	with connect_to_db() as conn:
		with conn.cursor() as cur:
			for file in files:
				file_path = os.path.join(path_to_folder, file)
				process_csv(file_path, file.split('.')[0], cur)
				conn.commit()

if __name__ == '__main__':
	main()