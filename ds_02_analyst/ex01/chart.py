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