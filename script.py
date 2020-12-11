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

def create_tables():
	pass 

def cleaning_up_data(file,conn):
	pass 

def insert_into_deliveries():
	pass 

def insert_into_operators():
	pass 

def insert_into_sectors():
	pass 

def insert_into_addresses():
	pass 

def insert_into_cities():
	pass 

def insert_into_energies():
	pass 

if __name__ == "__main__":
	conn = db_connection('final_project','sandrinevuachet','postgres','localhost',5432)
	cursor = conn.cursor()
	conn.autocommit = True
	pass 