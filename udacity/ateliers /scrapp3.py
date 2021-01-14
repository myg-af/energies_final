from bs4 import BeautifulSoup
import urllib.request,urllib.error
import csv


urlpage = "https://www.la-spa.fr/adopter-animaux?page="
page = urllib.request.urlopen(urlpage)
soup = BeautifulSoup(page, 'html.parser')

#req = urllib.request.Request(urlpage)
#try: urllib.request.urlopen(req)
#except urllib.error.HTTPError as e:
  #print(e.code)
  #print(e.read())

print(soup)


table = soup.find('div', attrs={'class': 'block-result-search-bottom'})
print('Number of results', len(results))


# create and write headers to a list
rows = []
rows.append(['Refuge', 'Name'])
#print(rows)

# loop over results
for result in table:
  refuge-name = table.find_all('span',{"class":"refuge-name"})
  print(refuge-name)


  # write columns to variables
  rank = data[0].getText()
  company = data[1].getText()
  location = data[2].getText()
  yearend = data[3].getText()
  salesrise = data[4].getText()
  sales = data[5].getText()
  staff = data[6].getText()
  comments = data[7].getText()


  # extract description from the name
  companyname = data[1].find('span', attrs={'class':'company-name'}).getText()
  description = company.replace(companyname, '')

  # remove unwanted characters
  sales = sales.strip('*').strip('†').replace(',','')



# go to link and extract company website
  url = data[1].find('a').get('href')
  page = urllib.request.urlopen(url)


  # parse the html
  soup = BeautifulSoup(page, 'html.parser')


  # find the last result in the table and get the link
  try:
    tableRow = soup.find('table').find_all('tr')[-1]
    webpage = tableRow.find('a').get('href')
  except:
    webpage = None

  # write each result to rows
  rows.append([rank, companyname, webpage, description, location, yearend, salesrise, sales, staff, comments])
  print(rows)


# Create csv and write rows to output file
with open('techtrack100.csv','w', newline='') as f_output:
  csv_output = csv.writer(f_output)
  csv_output.writerows(rows)
