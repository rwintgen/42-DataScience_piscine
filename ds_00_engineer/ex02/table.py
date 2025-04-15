import psycopg2

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def create_table(cur, table_name, headers):
	column_types = ['TIMESTAMPTZ', 'TEXT', 'INTEGER', 'NUMERIC(10, 2)', 'BIGINT', 'UUID']
	columns = ', '.join(f"{header} {col_type}" for header, col_type in zip(headers, column_types))
	query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
	cur.execute(query)

def main():
	file_path = './customer/data_2023_jan.csv'
	table_name = 'data_2023_jan'

	with open(file_path, 'r') as f:
		lines = [line.strip() for line in f if line.strip()]
		headers = lines[0].split(',')
		data_rows = [line.split(',') for line in lines[1:]]

	with connect_to_db() as conn:
		with conn.cursor() as cur:
			create_table(cur, table_name, headers)
			conn.commit()

if __name__ == '__main__':
	main()