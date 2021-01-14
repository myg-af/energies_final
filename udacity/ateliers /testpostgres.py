import csv
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup
import time
import psycopg2
from psycopg2 import OperationalError
from psycopg2 import connect, extensions, sql
import pandas as pd

page = "https://www.la-spa.fr/adopter-animaux?page="


#######################################################################################
# import the PostgreSQL adapter for Python

import psycopg2

# Connect to the PostgreSQL database server
postgresConnection = psycopg2.connect("dbname=spa2 user=sandrinevuachet password='postgres")
# Get cursor object from the database connection
cursor = postgresConnection.cursor()

name_Table  = "animal2"

# Create table statement

sqlCreateTable = "create table "+name_Table+" (id bigint, title varchar(128), summary varchar(256), story text);"

# Create a table in PostgreSQL database

cursor.execute(sqlCreateTable)

postgresConnection.commit()

# Get the updated list of tables

sqlGetTableList = "SELECT table_schema,table_name FROM information_schema.tables where table_schema='test' ORDER BY table_schema,table_name ;"

#sqlGetTableList = "\dt"

# Retrieve all the rows from the cursor

cursor.execute(sqlGetTableList)

tables = cursor.fetchall()

# Print the names of the tables

for table in tables:

    print(table)

################################################################################

def get_containers(page_number):
  my_url = page + str(page_number)
  print(my_url)
  uClient = uReq(my_url)
  page_html = uClient.read()
  uClient.close()
  print(my_url)
  page_soup = BeautifulSoup(page_html, "html.parser")
  return page_soup.findAll("div",{"class":"block-result-search-bottom"})


def get_sub_containers(container):
  #name = container.a.text
  refuge_name = container.find("span", {"class":"refuge-name"}).a.text
  #lien pour aller chercher les infos sur la seconde page
  link = container.find("span","animal-name")
  url2 = link.find("a").get("href")
  full_url2 = "https://www.la-spa.fr" + url2.strip()
  print(full_url2)

  page2 = uReq(full_url2).read()#enlever le read si résultat illisible
  soup = BeautifulSoup(page2, "html.parser")
  return soup.findAll("div",{"class":"fieldset-wrapper"})

def get_data(container):
  try:
    dob = container.find("div","field-name-field-date-naissance").text.strip()
    dob = dob.replace("Date Naissance:","")
    race = container.find("div","field-name-field-race").text.strip()
    race = race.replace("Race / Apparence:","")
    sex = container.find("div","field-name-field-sexe").text.strip()
    sex = sex.replace("Sexe:","")
    refuge = container.find("div","field-name-field-refuge-animal").text.strip()
    refuge = refuge.replace("Refuge:","")
    time.sleep(1)
    print("*"*150)
    print(dob + "\n" + race + "\n" + sex + "\n" + refuge)
    return [dob,race,sex,refuge]
  except ValueError:
    print("None")
  except AttributeError:
    print("None")

def prepare_data(file_name):
  with open(file_name,"w") as newFile:
    newFileWriter = csv.writer(newFile)
    newFileWriter.writerow(["DOB","race","sex","shelter"])

  for page_number in range ( 1,2):
    containers = get_containers(page_number)

    for container in containers:
      containers2 = get_sub_containers(container)

      for container in containers2:
        data = get_data(container)
  #print("name: " + name)
  #print("refuge_name: " + refuge_name)
        with open(file_name, "a") as newFile:
          newFileWriter = csv.writer(newFile)
          newFileWriter.writerow(data)

def write_db(file_name):
  df = pd.read_csv(file_name)
  print(df.head())

if __name__ == "__main__":
  file_name = "spa.csv"
  prepare_data(file_name)
  write_db(file_name)
