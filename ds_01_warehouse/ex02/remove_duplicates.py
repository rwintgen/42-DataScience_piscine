import psycopg2

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def rm_duplicates(table_name, cur):
	print("Removing duplicates...")
	query = f"""
		DELETE FROM {table_name}
		WHERE ctid NOT IN (
			SELECT MIN(ctid)
			FROM {table_name}
			GROUP BY event_time, event_type, product_id, price, user_id, user_session
		);
	"""
	cur.execute(query)

def main():
	path_to_folder = './customer'
	table_name = 'customers'

	try:
		with connect_to_db() as conn:
			with conn.cursor() as cur:
				rm_duplicates(table_name, cur)
				conn.commit()
				print(f"Removed duplicates from table: {table_name}")
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()