import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def connect_to_db():
	return psycopg2.connect(
		dbname='piscineds',
		user='rwintgen',
		password='mysecretpassword',
		host='localhost',
		port='5432'
	)

def fetch_frequency_data():
	query = """
		SELECT user_id, COUNT(*) AS frequency
		FROM customers
		WHERE event_type = 'purchase'
		GROUP BY user_id;
	"""
	with connect_to_db() as conn:
		df = pd.read_sql_query(query, conn)
	return df

def fetch_monetary_data():
	query = """
		SELECT user_id, SUM(price) AS monetary_value
		FROM customers
		WHERE event_type = 'purchase'
		GROUP BY user_id;
	"""
	with connect_to_db() as conn:
		df = pd.read_sql_query(query, conn)
	return df

def draw_frequency_bar_chart(df):
	sns.histplot(df['frequency'], bins=20, color='#A5C8E1', alpha=1.0, edgecolor='white', zorder=1000)
	plt.xlabel('Frequency')
	plt.ylabel('Customers')
	plt.xlim(0, 40)

	plt.xticks(range(0, 40, 10))

	plt.gca().set_facecolor('#ECECEC')
	plt.grid(color='white')

	for spine in plt.gca().spines.values():
		spine.set_visible(False)
	plt.tick_params(axis='both', which='both', length=0)

	plt.tight_layout()
	plt.show()

def draw_monetary_bar_chart(df):
	sns.histplot(df['monetary_value'], bins=range(0, 250, 50), color='#A5C8E1', alpha=1.0, edgecolor='white', zorder=1000)
	plt.xlabel('Monetary value in A$')
	plt.ylabel('Customers')
	plt.xlim(0, 250)

	plt.xticks(range(0, 250, 50))

	plt.gca().set_facecolor('#ECECEC')
	plt.grid(color='white')

	for spine in plt.gca().spines.values():
		spine.set_visible(False)
	plt.tick_params(axis='both', which='both', length=0)

	plt.tight_layout()
	plt.show()

def main():
	try:
		print("Fetching data...")
		frequency_df = fetch_frequency_data()
		monetary_df = fetch_monetary_data()
		print("fetched data")
		
		print("Generating charts...")
		draw_frequency_bar_chart(frequency_df)
		draw_monetary_bar_chart(monetary_df)
		print("Generated charts")

	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()