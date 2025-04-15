import os
import psycopg2

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def create_table(file_path, table_name, cur):
	with open(file_path, 'r') as f:
		headers = f.readline().strip().split(',')
		first_row = f.readline().strip().split(',')

	column_types = ['TIMESTAMPTZ', 'TEXT', 'INTEGER', 'NUMERIC(10, 2)', 'BIGINT', 'UUID']
	columns = ', '.join(f"{header} {col_type}" for header, col_type in zip(headers, column_types))
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