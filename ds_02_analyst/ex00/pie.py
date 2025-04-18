import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def count_event_type_values():
	query = """
		SELECT event_type, COUNT(*) AS count
		FROM customers
		GROUP BY event_type;
	"""
	with connect_to_db() as conn:
		df = pd.read_sql_query(query, conn)
	return df

def draw_chart(df):
	event_types = df['event_type']
	count = df['count']

	plt.pie(count, labels=event_types, autopct='%1.1f%%')
	plt.show()

def main():
	try:
		print("Generating pie chart...")
		df = count_event_type_values()
		draw_chart(df)
		print("Pie chart generated")
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()