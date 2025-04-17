import os
import psycopg2
import pandas as pd

# Only process the first MAX_ROWS rows of the csv files for memory protection
MAX_ROWS = 10000

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def create_and_populate_table(path_to_folder, table_name, cur):
	dataframes = []

	print("Creating combined table...")
	for file in os.listdir(path_to_folder):
		if file.endswith('.csv'):
			file_path = os.path.join(path_to_folder, file)
			df = pd.read_csv(file_path, nrows=MAX_ROWS)
			dataframes.append(df)
	combined_df = pd.concat(dataframes, ignore_index=True)
	combined_df = combined_df.where(pd.notnull(combined_df), None)

	column_types = ['TIMESTAMPTZ', 'TEXT', 'INTEGER', 'NUMERIC(10, 2)', 'BIGINT', 'UUID']
	columns = ', '.join(f"{header} {col_type}" for header, col_type in zip(combined_df.columns, column_types))
	create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
	cur.execute(create_query)

	print("Populating combined table...")
	for _, row in combined_df.iterrows():
		placeholders = ', '.join(['%s'] * len(row))
		insert_query = f"INSERT INTO {table_name} VALUES ({placeholders});"
		cur.execute(insert_query, tuple(row))

def main():
	path_to_folder = './customer'
	table_name = 'customers'

	try:
		with connect_to_db() as conn:
			with conn.cursor() as cur:
				create_and_populate_table(path_to_folder, table_name, cur)
				conn.commit()
				print(f"Table created and populated: {table_name}")
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()