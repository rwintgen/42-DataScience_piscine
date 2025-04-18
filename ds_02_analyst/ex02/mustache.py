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

def fetch_price_data():
	query = """
		SELECT price
		FROM customers
		WHERE event_type = 'purchase';
	"""
	with connect_to_db() as conn:
		df = pd.read_sql_query(query, conn)
	return df

def fetch_avg_basket_price_per_user():
	query = """
		SELECT user_id, AVG(price) AS avg_basket_price
		FROM customers
		WHERE event_type = 'purchase'
		GROUP BY user_id;
	"""
	with connect_to_db() as conn:
		df = pd.read_sql_query(query, conn)
	return df

def calc_price_stats(df):
	stats = {
		'count': df['price'].count(),
		'mean': df['price'].mean(),
		'median': df['price'].median(),
		'min': df['price'].min(),
		'max': df['price'].max(),
		'qtl25': df['price'].quantile(0.25),
		'qtl50': df['price'].quantile(0.50),
		'qtl75': df['price'].quantile(0.75)
	}
	return stats

def print_price_stats(stats):
	for key, value in stats.items():
		print(f"{key}\t\t {value}")

def draw_price_box_plot(df):
	sns.boxplot(x=df['price'], color='gray')
	plt.xlabel('Price')

	plt.gca().set_facecolor('#ECECEC')
	plt.grid(color='white', axis='x')

	for spine in plt.gca().spines.values():
		spine.set_visible(False)
	plt.tick_params(axis='both', which='both', length=0)

	plt.tight_layout()
	plt.show()

def draw_price_quantiles_box_plot(stats):
    sns.boxplot(data=[[stats['min'], stats['qtl25'], stats['qtl50'], stats['qtl75']]], color='#78B075', orient='h')
    plt.xlabel('Price')

    plt.gca().set_facecolor('#ECECEC')
    plt.grid(color='white', axis='x')

    for spine in plt.gca().spines.values():
        spine.set_visible(False)
    plt.tick_params(axis='both', which='both', length=0)

    plt.tight_layout()
    plt.show()

def draw_avg_baset_price_box_plot(df):
	sns.boxplot(x=df['avg_basket_price'], color='#73A5C6')
	plt.xlim(0, 50)

	plt.gca().set_facecolor('#ECECEC')
	plt.grid(color='white', axis='x')

	for spine in plt.gca().spines.values():
		spine.set_visible(False)
	plt.tick_params(axis='both', which='both', length=0)

	plt.tight_layout()
	plt.show()

def main():
	try:
		print("Printing price stats...")
		df_price = fetch_price_data()
		stats = calc_price_stats(df_price)
		print_price_stats(stats)
		print("Printed price stats")
		
		print("Generating charts...")
		draw_price_box_plot(df_price)
		draw_price_quantiles_box_plot(stats)
		df_avg_basket = fetch_avg_basket_price_per_user()
		draw_avg_baset_price_box_plot(df_avg_basket)
		print("Generated charts")
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()