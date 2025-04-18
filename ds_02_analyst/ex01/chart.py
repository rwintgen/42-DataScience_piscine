import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def fetch_customers_per_day():
	query = """
		SELECT DATE_TRUNC('day', event_time) AS day, COUNT(DISTINCT user_id) AS customer_count
		FROM customers
		WHERE event_type = 'purchase'
		GROUP BY day
		ORDER BY day;
	"""
	with connect_to_db() as conn:
		df = pd.read_sql_query(query, conn)
	return df



def draw_customers_per_day_chart(df):
	plt.plot(df['day'], df['customer_count'], color='#00008B', label='Customers')
	plt.ylabel('Number of Customers')

	# Display date formatted as month on x-axis
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b'))
	plt.gca().xaxis.set_major_locator(mdates.MonthLocator())

	# Color adjustments for background and grid
	plt.gca().set_facecolor('#ECECEC')
	plt.grid(color='white')

	# Remove chart borders
	for spine in plt.gca().spines.values():
		spine.set_visible(False)

	plt.show()



def main():
	try:
		print("Generating charts...")
		customers_df = fetch_customers_per_day()
		draw_customers_per_day_chart(customers_df)
		plt.show(block=False)


		print("Charts generated")
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()