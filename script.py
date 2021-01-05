import psycopg2
from psycopg2 import OperationalError as e
import pandas as pd
from sqlalchemy import create_engine
import datetime

####################################################################################################################

#import data from CSV using pandas
elec_2018 = pd.read_csv('./data/elec/donnees_elec_adresse_2018.csv',sep=';',encoding="utf8")
elec_2019 = pd.read_csv('./data/elec/donnees_elec_adresse_2019.csv',sep=';',encoding="utf8")
gas_2018 = pd.read_csv('./data/gaz/donnees_gaz_adresse_2018.csv',sep=';',encoding="utf8")
gas_2019 = pd.read_csv('./data/gaz/donnees_gaz_adresse_2018.csv',sep=';',encoding="utf8")
#hot_cold_2018 = pd.read_csv('./data/hot_cold/',sep=';')
#hot_cold_2019 = pd.read_csv('./data/hot_cold/',sep=';')

####################################################################################################################

# connecting db using psycopg2
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

####################################################################################################################

#creating tables using psycopg2
def create_tables():
	try:
		cursor.execute("""CREATE TABLE IF NOT EXISTS "operators" (
			"id" SERIAL PRIMARY KEY,
			"name" varchar
		);

		CREATE TABLE IF NOT EXISTS "deliveries" (
		  "id" SERIAL PRIMARY KEY,
		  "year" int,
		  "iris_code" varchar,
		  "consumption" varchar,
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
		  "name" varchar UNIQUE
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


def extra_table():

	cursor.execute("""CREATE TABLE IF NOT EXISTS "all_data" (
		"id" SERIAL PRIMARY KEY,
		"operators" varchar, 
		"year" int, 
		"energy" varchar, 
		"code_iris" varchar, 
		"address" varchar, 
		"city" varchar,
	    "code" varchar, 
	    "consu" varchar, 
	    "pdl" int
       	);
	""")

####################################################################################################################

#concat electricity 2018 and 2019 dataframes using pandas 
def concatenate_electricity(file,file2):
	file = file.head(15100)
	file2 = file2.head(15100)
	file = file[['OPERATEUR', 'ANNEE', 'FILIERE', 'CODE_IRIS','ADRESSE', 'NOM_COMMUNE',
       'CODE_GRAND_SECTEUR', 'CONSO', 'PDL']]
	elec = [file, file2]
	elec = pd.concat(elec)
	elec.reset_index(drop=True, inplace=True)
	return elec

#concat gas 2018 and 2019 dataframes using pandas 
def concatenate_gas(file,file2):
	file = file.head(15100)
	file2 = file2.head(15100)
	gas = [file, file2]
	gas = pd.concat(gas)
	gas.reset_index(drop=True, inplace=True)
	return gas

#concat hot and cold 2018 and 2019 dataframes using pandas 
# def concatenate_hot_cold(file,file2):
# 	file = file.head(7500)
# 	file2 = file2.head(7500)
# 	print(file)
# 	print('*****************************')
# 	print(file2)
# 	#file = file[['OPERATEUR', 'ANNEE', 'FILIERE', 'CODE_IRIS','ADRESSE', 'NOM_COMMUNE',
#        #'CODE_GRAND_SECTEUR', 'CONSO', 'PDL']]
# 	#print('*****************************')
# 	#print(file)
# 	hot_cold = [file, file2]
# 	hot_cold = pd.concat(hot_cold)
# 	print('*****************************')
# 	print(hot_cold)
# 	print('*****************************')
# 	return hot_cold

####################################################################################################################

def all_data_table():

	table = [elec,gas]
	table = pd.concat(table)
	#print(table)
	insertion(table)

def insertion(table):
	for row in table.values:
		print(row)
		query2 = """SELECT * FROM all_data WHERE operators = (%s) and year = (%s) and energy = (%s) and code_iris = (%s) 
			and address = (%s) and city = (%s) and code = (%s) and consu = (%s) and pdl = (%s)"""
		cursor.execute(query2,(f'{row[0]}',f'{row[1]}',f'{row[2]}',f'{row[3]}',f'{row[4]}',f'{row[5]}',
				f'{row[6]}',f'{row[7]}',f'{row[8]}'))
		result = cursor.fetchall()
		print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO all_data (operators,year,energy,code_iris,address,city,code,consu,pdl) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"
			cursor.execute(query,(f'{row[0]}',f'{row[1]}',f'{row[2]}',f'{row[3]}',f'{row[4]}',f'{row[5]}',
				f'{row[6]}',f'{row[7]}',f'{row[8]}'))
			print(query)

####################################################################################################################

#TABLE OPERATORS
#inserting data from dataframes into tables using psycopg2
def insert_into_operators():

	elec_table = elec['OPERATEUR'].drop_duplicates()
	gas_table = gas['OPERATEUR'].drop_duplicates()
	table = [elec_table,gas_table]
	table = pd.concat(table).rename("name").to_frame()
	print(table['name'].values)
	print('*****************************')
	insertifnotexists5(table)

def insertifnotexists5(table):
	for name in table['name'].values:
		query2 = """SELECT name FROM operators WHERE name = (%s)"""
		cursor.execute(query2,(f'{name}',))
		result = cursor.fetchall()
		print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO operators (name) VALUES (%s);"
			cursor.execute(query,(f'{name}',))
			print(query)

####################################################################################################################

#TABLE SECTORS 
#inserting data from dataframes into tables using psycopg2
def insert_into_sectors():
	#from initial dfs select only operator column
	elec_table = elec['CODE_GRAND_SECTEUR'].drop_duplicates()
	gas_table = gas['CODE_GRAND_SECTEUR'].drop_duplicates()
	# concat both df to get one
	table = [elec_table,gas_table]
	table = pd.concat(table).rename("name").to_frame()
	print(table['name'].values)
	print('*****************************')
	insertifnotexists4(table)

def insertifnotexists4(table):
	for name in table['name'].values:	
		query2 = """SELECT name FROM sectors WHERE name = (%s)"""
		cursor.execute(query2,(f'{name}',))
		result = cursor.fetchall()
		#print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO sectors (name) VALUES (%s);"
			cursor.execute(query,(f'{name}',))

####################################################################################################################

#TABLE ADDRESSES
#inserting data from dataframes into tables using psycopg2
def insert_into_addresses():

	query_sql = """SELECT DISTINCT ad.address, c.id as city_id from all_data ad join cities c on ad.city = c.name """
	cursor.execute(query_sql)
	result = cursor.fetchall()
	print(result[1])

	for row in result:
		query2 = """SELECT name,city_id FROM addresses WHERE name = (%s) and city_id = (%s)"""
		cursor.execute(query2,(f'{row[0]}',f'{row[1]}'))
		result = cursor.fetchall()
		#print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO addresses (name,city_id) VALUES (%s,%s);"
			cursor.execute(query,(f'{row[0]}',f'{row[1]}'))

####################################################################################################################

# TABLE DELIVERIES
#inserting data from dataframes into tables using psycopg2
def insert_into_deliveries():

	query = """SELECT DISTINCT ad.year, ad.code_iris,ad.consu, s.id as sector_id, 
		o.id as operator_id,e.id as energy_id,a.id as address_id
		from all_data ad 
		join sectors s on s.name = ad.code 
		join operators o on ad.operators = o.name 
		join energies e on ad.energy = e.name 
		join addresses a on ad.address = a.name""" 
	cursor.execute(query)
	result = cursor.fetchall()
	print(result[0])
	print("*******************************$")
	print(result[0][0])
	print(result[0][1])
	print(result[0][2])
	print(result[0][3])
	print(result[0][4])
	print(result[0][5])
	print(result[0][6])
	for row in result:
		query2 = """SELECT * FROM deliveries WHERE year = (%s) and iris_code = (%s) and consumption = (%s) 
			and sector_id = (%s) and operator_id = (%s) and energy_id = (%s) and address_id = (%s) """
		cursor.execute(query2,(f'{row[0]}',f'{row[1]}',f'{row[2]}',f'{row[3]}',f'{row[4]}',f'{row[5]}',f'{row[6]}'))
		result = cursor.fetchall()
		#print(result[0])
		#print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO deliveries (year,iris_code,consumption,sector_id,operator_id,energy_id,address_id) VALUES (%s,%s,%s,%s,%s,%s,%s);"
			cursor.execute(query,(f'{row[0]}',f'{row[1]}',f'{row[2]}',f'{row[3]}',f'{row[4]}',f'{row[5]}',f'{row[6]}'))


####################################################################################################################

#TABLE CITIES
#inserting data from dataframes into tables using psycopg2
def insert_into_cities():

	elec_table = elec['NOM_COMMUNE'].drop_duplicates()
	gas_table = gas['NOM_COMMUNE'].drop_duplicates()
	# concat both df to get one
	table = [elec_table,gas_table]
	table = pd.concat(table).rename("name").to_frame()
	print(table['name'].values)
	print('*****************************')
	insertifnotexists0(table)

def insertifnotexists0(table):
	for name in table['name'].values:
		query2 = """SELECT name FROM cities WHERE name = (%s)"""
		cursor.execute(query2,(f'{name}',))
		result = cursor.fetchall()
		#print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO cities (name) VALUES (%s);"
			cursor.execute(query,(f'{name}',))



# 	elec_table = elec[['NOM_COMMUNE','CODE_IRIS']].drop_duplicates()
# 	gas_table = gas[['NOM_COMMUNE','CODE_IRIS']].drop_duplicates()
# 	# concat both df to get one
# 	table = [elec_table,gas_table]
# 	table = pd.concat(table)
# 	#print(table)
# 	#print('*****************************')
# 	print(table.info())
# 	insertifnotexists2(table)

# def insertifnotexists2(table):
# 	#print(type(table.values[0][0]))
# 	#print(type(table.values[0][1]))
# 	for row in table.values:
# 		query2 = """SELECT name, code_iris FROM cities WHERE name = %s AND code_iris = %s;"""
# 		cursor.execute(query2,(f'{row[0]}',f'{row[1]}'))
# 		result = cursor.fetchall()
# 		#print(result)
# 		#print(len(result) == 0)
# 		if len(result) == 0 :
# 			query = "INSERT INTO cities (name,code_iris) VALUES (%s,%s);"
# 			cursor.execute(query,(f'{row[0]}',f'{row[1]}'))
# 			#cursor.execute(query2,(row[0],row[1]))
# 			#print(cursor.query)
# 			conn.commit()

####################################################################################################################

#TABLE ENERGIES
#inserting data from dataframes into tables using psycopg2
def insert_into_energies():
	elec_table = elec['FILIERE'].drop_duplicates()
	gas_table = gas['FILIERE'].drop_duplicates()
	# concat both df to get one
	table = [elec_table,gas_table]
	table = pd.concat(table).rename("name").to_frame()
	print(table['name'].values)
	print('*****************************')
	insertifnotexists1(table)

def insertifnotexists1(table):
	for name in table['name'].values:
		query2 = """SELECT name FROM energies WHERE name = (%s)"""
		cursor.execute(query2,(f'{name}',))
		result = cursor.fetchall()
		#print(len(result) == 0)
		if len(result) == 0 : 
			query = "INSERT INTO energies (name) VALUES (%s);"
			cursor.execute(query,(f'{name}',))


####################################################################################################################

if __name__ == "__main__":
	print(__name__)

	conn = db_connection('final_project','sandrinevuachet','postgres','localhost',5432)
	cursor = conn.cursor()
	conn.autocommit = True
	#create_tables()
	#extra_table()
	# engine = alchemy_connection()

	elec = concatenate_electricity(elec_2018,elec_2019)
	gas = concatenate_gas(gas_2018,gas_2019)
	#hot_cold = concatenate_hot_cold(hot_cold_2018,hot_cold_2019)
	#all_data_table()
	#insert_into_operators()
	#insert_into_sectors()
	#insert_into_energies()
	#insert_into_cities()
	#insert_into_addresses()
	#insert_into_deliveries()