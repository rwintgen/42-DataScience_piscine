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

def main():

if __name__ == '__main__':
	main()