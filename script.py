import psycopg2
from psycopg2 import OperationalError
import pandas as pd
from sqlalchemy import create_engine
import datetime

def db_connection(db, user, password, host, port):
	try:
		connection = psycopg2.connect(
		database = db,
        user = user,
        host = host,
        password = password,
        port = port
		)
		print(f'successfully connected to {db}')
	except e:
		print(f'Could not find any DB named {db}')
	return connection

def cleaning_up_data(file,conn):
	pass 

def create_table_deliveries():
	pass 

def create_table_operators():
	pass 

def create_table_sectors():
	pass 

def create_table_addresses():
	pass 

def create_table_cities():
	pass 

def create_table_energies():
	pass 

if __name__ == "__main__":
	conn = db_connection('final_project','sandrinevuachet','postgres','localhost',5432)
	cursor = conn.cursor()
	conn.autocommit = True
	pass 