
import csv
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup

url = "https://www.la-spa.fr/adopter-animaux?page="
soup = soup(url, 'html.parser')

def get_pages(url, nb):
    pages = []
    for page in range(1,nb+1):
        text = url + str(page)
        pages.append(text)

    for x in pages:
      print("ma page : " + pages[pages.index(x)])

pages = get_pages(url,201)

