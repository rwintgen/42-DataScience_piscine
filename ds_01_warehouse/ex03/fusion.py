import os
import psycopg2
import pandas as pd

# Only add the first MAX_ROWS rows of the csv files to database
MAX_ROWS = 10000

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def create_and_populate_table(path_to_file, cur):
	dataframes = []
	table_name = 'tmp'

	print("Creating temporary item table...")
	df = pd.read_csv(path_to_file, nrows=MAX_ROWS)

	column_types = ['INTEGER', 'NUMERIC', 'TEXT', 'TEXT']
	columns = ', '.join(f"{header} {col_type}" for header, col_type in zip(df.columns, column_types))
	create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
	cur.execute(create_query)

	print("Populating temporary item table...")
	for _, row in df.iterrows():
		placeholders = ', '.join(['%s'] * len(row))
		insert_query = f"INSERT INTO {table_name} VALUES ({placeholders});"
		cur.execute(insert_query, tuple(row))

# def combine_tables(cur):


def main():
	path_to_file = './item/item.csv'

	try:
		with connect_to_db() as conn:
			with conn.cursor() as cur:
				create_and_populate_table(path_to_file, cur)
				# combine_tables(cur)
				conn.commit()
				print(f"Combined tables: ")
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()