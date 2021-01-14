import urllib2
from bs4 import BeautifulSoup
import csv
import os

out=open("proba.csv","rb")
data=csv.reader(out)

def make_soup(url):

    thepage = urllib2.urlopen(url)
    soupdata = BeautifulSoup(thepage, "html.parser")
    return soupdata

maindatatable=""
soup = make_soup("")

for record in soup.findAll('tr'):

    datatable=""
    for data in record.findAll('td'):
      datatable=datatable+";"+data.text

    maindatatable = maindatatable + "\n" + datatable[1:]

header = "DOB;race;sex;refuge"

print maindatatable

file = open(os.path.expanduser("proba.csv"),"wb")

utf16_str1 =header.encode('utf16')
utf16_str2 = maindatatable.encode('utf16')

file.write(utf16_str1)
file.write(utf16_str2)
file.close()

make_soup('')
