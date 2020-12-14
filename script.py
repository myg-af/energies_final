import psycopg2
from psycopg2 import OperationalError
import pandas as pd
from sqlalchemy import create_engine
import datetime

#data
elec_2018 = pd.read_csv('./data/elec/donnees_elec_adresse_2018.csv',sep=';')
elec_2019 = pd.read_csv('./data/elec/donnees_elec_adresse_2019.csv',sep=';')
gas_2018 = pd.read_csv('./data/gaz/donnees_gaz_adresse_2018.csv',sep=';')
gas_2019 = pd.read_csv('./data/gaz/donnees_gaz_adresse_2018.csv',sep=';')
#hot_cold_2018 = pd.read_csv('./data/hot_cold/',sep=';')
#hot_cold_2019 = pd.read_csv('./data/hot_cold/',sep=';')

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
	try:
		cursor.execute("""CREATE TABLE IF NOT EXISTS "operators" (
			"id" SERIAL PRIMARY KEY,
			"name" varchar
		);

		CREATE TABLE IF NOT EXISTS "deliveries" (
		  "id" SERIAL PRIMARY KEY,
		  "date" date,
		  "code" int,
		  "territory" varchar,
		  "consumption" float8,
		  "sector_id" int,
		  "operator_id" int,
		  "energy_id" int,
		  "address_id" int
		);

		CREATE TABLE IF NOT EXISTS "energies" (
		  "id" SERIAL PRIMARY KEY,
		  "name" varchar UNIQUE
		);

		CREATE TABLE IF NOT EXISTS "sectors" (
		  "id" SERIAL PRIMARY KEY,
		  "name" varchar UNIQUE
		);

		CREATE TABLE IF NOT EXISTS "cities" (
		  "id" SERIAL PRIMARY KEY,
		  "name" varchar UNIQUE, 
		  "code_iris" int
		);

		CREATE TABLE IF NOT EXISTS "addresses" (
		  "id" SERIAL PRIMARY KEY,
		  "name" varchar,
		  "city_id" int
		);

		ALTER TABLE "deliveries" ADD FOREIGN KEY ("operator_id") REFERENCES "operators" ("id");
		ALTER TABLE "deliveries" ADD FOREIGN KEY ("address_id") REFERENCES "addresses" ("id");
		ALTER TABLE "deliveries" ADD FOREIGN KEY ("energy_id") REFERENCES "energies" ("id");
		ALTER TABLE "deliveries" ADD FOREIGN KEY ("sector_id") REFERENCES "sectors" ("id");
		ALTER TABLE "addresses" ADD FOREIGN KEY ("city_id") REFERENCES "cities" ("id");
		""")
		print('Well done, there are all created')
	except e:
		print('Try again :(')


def electricity(file,file2):
	#print(file)
	#print('*****************************')
	#print(file2)
	file = file[['OPERATEUR', 'ANNEE', 'FILIERE', 'CODE_IRIS','ADRESSE', 'NOM_COMMUNE',
       'CODE_GRAND_SECTEUR', 'CONSO', 'PDL']]
	#print('*****************************')
	#print(file)
	elec = [file, file2]
	elec = pd.concat(elec)
	#print('*****************************')
	#print(elec)
	#print('*****************************')
	return elec

def gas(file,file2):
	#print(file)
	#print('*****************************')
	#print(file2)
	#file = file[['OPERATEUR', 'ANNEE', 'FILIERE', 'CODE_IRIS','ADRESSE', 'NOM_COMMUNE',
       #'CODE_GRAND_SECTEUR', 'CONSO', 'PDL']]
	#print('*****************************')
	#print(file)
	gas = [file, file2]
	gas = pd.concat(gas)
	#print('*****************************')
	#print(gas)
	#print('*****************************')
	return gas

def hot_cold(file,file2):
	print(file)
	print('*****************************')
	print(file2)
	#file = file[['OPERATEUR', 'ANNEE', 'FILIERE', 'CODE_IRIS','ADRESSE', 'NOM_COMMUNE',
       #'CODE_GRAND_SECTEUR', 'CONSO', 'PDL']]
	#print('*****************************')
	#print(file)
	hot_cold = [file, file2]
	hot_cold = pd.concat(hot_cold)
	print('*****************************')
	print(hot_cold)
	print('*****************************')
	return hot_cold

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
	print(__name__)
	#print(elec_2019)
	conn = db_connection('final_project','sandrinevuachet','postgres','localhost',5432)
	cursor = conn.cursor()
	conn.autocommit = True
	#create_tables()

	elec = electricity(elec_2018,elec_2019)
	gas = gas(gas_2018,gas_2019)
	#hot_cold = hot_cold(hot_cold_2018,hot_cold_2019)
