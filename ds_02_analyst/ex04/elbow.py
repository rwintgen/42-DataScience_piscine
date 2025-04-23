import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

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

def normalize_data(df):
	scaler = StandardScaler()
	X = df[['frequency', 'monetary_value']]
	return scaler.fit_transform(X)

def calculate_wcss(X_scaled):
	wcss = []
	for k in range(1, 11):
		kmeans = KMeans(n_clusters=k, random_state=42)
		kmeans.fit(X_scaled)
		wcss.append(kmeans.inertia_)
	return wcss

def draw_elbow_method(wcss):
	plt.plot(range(1, 11), wcss, color='blue')
	plt.xlabel('Number of clusters')
	
	plt.gca().set_facecolor('#ECECEC')
	plt.grid(color='white')

	for spine in plt.gca().spines.values():
		spine.set_visible(False)

	plt.tight_layout()
	plt.show()

def perform_elbow_method():
	frequency_df = fetch_frequency_data()
	monetary_df = fetch_monetary_data()
	df = pd.merge(frequency_df, monetary_df, on='user_id')
	X_scaled = normalize_data(df)
	wcss = calculate_wcss(X_scaled)
	draw_elbow_method(wcss)

def main():
	try:
		print("Performing Elbow Method...")
		perform_elbow_method()
		print("Elbow Method completed")
	except Exception as e:
		print(f"Error: {e}")

if __name__ == '__main__':
	main()