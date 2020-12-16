import psycopg2
from psycopg2 import OperationalError as e
import pandas as pd
from sqlalchemy import create_engine
import datetime

#data
elec_2018 = pd.read_csv('./data/elec/donnees_elec_adresse_2018.csv',sep=';',encoding="utf8")
elec_2019 = pd.read_csv('./data/elec/donnees_elec_adresse_2019.csv',sep=';',encoding="utf8")
gas_2018 = pd.read_csv('./data/gaz/donnees_gaz_adresse_2018.csv',sep=';',encoding="utf8")
gas_2019 = pd.read_csv('./data/gaz/donnees_gaz_adresse_2018.csv',sep=';',encoding="utf8")
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

# def alchemy_connection():
#     db ="postgresql://sandrinevuachet:postgres@localhost:5432/final_project"
#     engine = create_engine(db, client_encoding='utf8')
#     return engine

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
		  "name" varchar , 
		  "code_iris" varchar,
		  UNIQUE("name","code_iris")
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


def concatenate_electricity(file,file2):
	file = file[['OPERATEUR', 'ANNEE', 'FILIERE', 'CODE_IRIS','ADRESSE', 'NOM_COMMUNE',
       'CODE_GRAND_SECTEUR', 'CONSO', 'PDL']]
	elec = [file, file2]
	elec = pd.concat(elec)
	return elec

def concatenate_gas(file,file2):
	gas = [file, file2]
	gas = pd.concat(gas)
	return gas

def concatenate_hot_cold(file,file2):
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

	#from initial dfs select only operator column
	elec_table = elec['OPERATEUR'].drop_duplicates()
	gas_table = gas['OPERATEUR'].drop_duplicates()
	# concat both df to get one
	table = [elec_table,gas_table]
	table = pd.concat(table).rename("name").to_frame()
	#print(table['name'].values)
	#print('*****************************')
	insertifnotexists(table)

def insertifnotexists(table):
	for name in table['name'].values:
		query2 = """SELECT name FROM operators WHERE name = (%s)"""
		cursor.execute(query2,(f'{name}',))
		result = cursor.fetchall()
		#print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO operators (name) VALUES (%s);"
			cursor.execute(query,(f'{name}',))

def insert_into_sectors():
	#from initial dfs select only operator column
	elec_table = elec['CODE_GRAND_SECTEUR'].drop_duplicates()
	gas_table = gas['CODE_GRAND_SECTEUR'].drop_duplicates()
	# concat both df to get one
	table = [elec_table,gas_table]
	table = pd.concat(table).rename("name").to_frame()
	print(table['name'].values)
	print('*****************************')
	insertifnotexists(table)

def insertifnotexists(table):
	for name in table['name'].values:
		query2 = """SELECT name FROM sectors WHERE name = (%s)"""
		cursor.execute(query2,(f'{name}',))
		result = cursor.fetchall()
		#print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO sectors (name) VALUES (%s);"
			cursor.execute(query,(f'{name}',))

def insert_into_addresses():
	pass 

def insert_into_cities():
	elec_table = elec[['NOM_COMMUNE','CODE_IRIS']].drop_duplicates()
	gas_table = gas[['NOM_COMMUNE','CODE_IRIS']].drop_duplicates()
	# concat both df to get one
	table = [elec_table,gas_table]
	table = pd.concat(table)
	#print(table)
	#print('*****************************')
	print(table.info())
	insertifnotexists2(table)


def insertifnotexists2(table):
	#print(type(table.values[0][0]))
	#print(type(table.values[0][1]))
	for row in table.values:
		query2 = """SELECT name, code_iris FROM cities WHERE name = %s AND code_iris = %s;"""
		cursor.execute(query2,(f'{row[0]}',f'{row[1]}'))
		result = cursor.fetchall()
		#print(result)
		#print(len(result) == 0)
		if len(result) == 0 :
			query = "INSERT INTO cities (name,code_iris) VALUES (%s,%s);"
			cursor.execute(query,(f'{row[0]}',f'{row[1]}'))
			#cursor.execute(query2,(row[0],row[1]))
			#print(cursor.query)
			conn.commit()


def insert_into_energies():
	elec_table = elec['FILIERE'].drop_duplicates()
	gas_table = gas['FILIERE'].drop_duplicates()
	# concat both df to get one
	table = [elec_table,gas_table]
	table = pd.concat(table).rename("name").to_frame()
	print(table['name'].values)
	print('*****************************')
	insertifnotexists(table)

def insertifnotexists(table):
	for name in table['name'].values:
		query2 = """SELECT name FROM energies WHERE name = (%s)"""
		cursor.execute(query2,(f'{name}',))
		result = cursor.fetchall()
		#print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO energies (name) VALUES (%s);"
			cursor.execute(query,(f'{name}',))

if __name__ == "__main__":
	print(__name__)

	conn = db_connection('final_project','sandrinevuachet','postgres','localhost',5432)
	cursor = conn.cursor()
	conn.autocommit = True
	create_tables()
	# engine = alchemy_connection()

	elec = concatenate_electricity(elec_2018,elec_2019)
	gas = concatenate_gas(gas_2018,gas_2019)
	#hot_cold = concatenate_hot_cold(hot_cold_2018,hot_cold_2019)
	#insert_into_operators()
	#insert_into_sectors()
	#insert_into_energies()
	insert_into_cities()