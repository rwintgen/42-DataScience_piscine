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

def fetch_total_sales_per_month():
	query = """
		SELECT DATE_TRUNC('month', event_time) AS month, SUM(price) / 1000000 AS total_sales_millions
		FROM customers
		WHERE event_type = 'purchase'
		GROUP BY month
		ORDER BY month;
	"""
	with connect_to_db() as conn:
		df = pd.read_sql_query(query, conn)
	return df

def fetch_avg_spend_per_customer():
	query = """
		SELECT DATE_TRUNC('month', event_time) AS month, SUM(price) / COUNT(DISTINCT user_id) AS avg_spend
		FROM customers
		WHERE event_type = 'purchase'
		GROUP BY month
		ORDER BY month;
	"""
	with connect_to_db() as conn:
		df = pd.read_sql_query(query, conn)
	return df

def draw_customers_per_day_chart(df):
	plt.plot(df['day'], df['customer_count'], color='#00008B')
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
	plt.tick_params(axis='both', which='both', length=0)

	plt.tight_layout()
	plt.show()

def draw_total_sales_chart(df):
	plt.bar(df['month'], df['total_sales_millions'], color='#B7C9E2', width=25, zorder=1000)
	plt.ylabel('Total sales in million of A$')

	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b'))
	plt.gca().xaxis.set_major_locator(mdates.MonthLocator())

	plt.gca().set_facecolor('#ECECEC')
	plt.grid(color='white', axis='y')

	for spine in plt.gca().spines.values():
		spine.set_visible(False)
	plt.tick_params(axis='both', which='both', length=0)

	plt.tight_layout()
	plt.show()

def draw_avg_spend_chart(df):
	plt.fill_between(df['month'], df['avg_spend'], color='#B7C9E2')
	plt.title('Average spend/cutomer in A$')
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b'))
	plt.gca().xaxis.set_major_locator(mdates.MonthLocator())

	plt.gca().set_facecolor('#ECECEC')
	plt.grid(color='white')

	for spine in plt.gca().spines.values():
		spine.set_visible(False)
	plt.tick_params(axis='both', which='both', length=0)

	plt.tight_layout()
	plt.show()

def main():
	try:
		print("Generating charts...")
		customers_df = fetch_customers_per_day()
		draw_customers_per_day_chart(customers_df)

		sales_df = fetch_total_sales_per_month()
		draw_total_sales_chart(sales_df)

		avg_spend_df = fetch_avg_spend_per_customer()
		draw_avg_spend_chart(avg_spend_df)
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()