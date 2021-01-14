#Quizz scrapping

#Q1: Quelles librairies Python nous permmettent de scrapper des données sur les pages web? (indice: tapez "scraping python" dans Google)
#   - Requests
#   - Beautiful Soup

#Q2: Une librairie que nous avons déjà utilisée en début de formation nous permet également de scraper des données sur une page web. Laquelle?
#   Pandas

#Q3: 
# a: Que fait la fonction .read_html de Pandas? Elle lit la page html et en extrait des données sous forme de dataFrame
# b: Quels éléments la fonction recherche-t-elle dans un document html?
# This function searches for <table> elements and only for <tr> and <th> rows and <td> elements within each <tr> or <th> element in the table.
# c: Quel type d'objet la fonction renvoit-elle? Un dataFrame

#Q4: La librairie Requests
# a: À quoi sert cette librairie? Requête html
# b: Que permet-elle d'obtenir dans une action de scraping de page web? 
# Elle permet d'obtenir la réponse du serveur en plus du contenue de la requête


#Q5: La librairie Beautiful Soup
# a: À quoi sert cette librairie? Elle permet de parser et d'extraire des informations obtenu par requêtage html par exemple
# b: Peut-elle être utilisée seule pour scraper une page web en ligne? 
# Non car elle ne fait pas de requête. Mais oui sur un document html en local grace à open()
# c: Quel est son intérêt par rapport à Requests? 
# Elle permet d'aller plus en profondeur dans la recherche d'info quand Requests se contente de la réponse serveur



#PARTIE A

#À votre tour de jouer! Nous allons acquérir ensemble des données brutes, les travailler, les nettoyer.

#Voici la premère page web visée.
url = 'https://coronavirus.jhu.edu/data/mortality'
#Entrz l'url dans votre navigateur web et observer la page.
#Nous souhaitons scrapper le tableau contenant les données sur le coronavirus dans le monde
import pandas as pd
# 1: scrapper le tableau de données. Quelle méthode allez-vous utiliser? df.read_html(url)
data = pd.read_html(url)
print("type de l'objet 'data' = " + str(type(data)))
print('taille de la list de dataframe obtenue = '+ str(len(data)))
print(data[0])
print('-----------------------------------------')
# 
# 2: Maintenant que vous avez ce tableau, nous souhaitons avoir un tableau des 10 pays ayant eu le plus de morts ("Deaths").
# Enregister ce dataFrame dans une variable. Quel est le type de cette variable?
deaths_coronavirus = data[0].sort_values(by='Deaths', ascending=False).head(10)
print(deaths_coronavirus)
print(type(deaths_coronavirus))
print('-----------------------------------------')
# 
# 3: Remplacer le nom des pays par le continent. Supprimer les colonnes inutilisées.
# Asie, Europe, Amerique du Nord, Amerique du Sud, Afrique, Antartique
# Exemple => dic = {'US':'Amerique du Nord','Brazil':'Amerique du Sud','Mexico':'Amerique du Nord','India':'Asie','United Kingdom':'Europe','Italy':'Europe','France':'Europe','Spain':'Europe','Peru':'Amerique du Sud','Iran':'Asie'}
deaths_by_cont = deaths_coronavirus.replace({'US':'Amerique du Nord','Brazil':'Amerique du Sud','Mexico':'Amerique du Nord','India':'Asie','United Kingdom':'Europe','Italy':'Europe','France':'Europe','Spain':'Europe','Peru':'Amerique du Sud','Iran':'Asie'}).drop(columns=['Confirmed','Case-Fatality','Deaths/100K pop.']).reset_index(drop=True)
print(deaths_by_cont)
print('-----------------------------------------')
# 4: Obtenir une somme par continent du nombre de mort. Enregistrer ce tableau dans une nouvelle variable.
print(deaths_by_cont.groupby('Country').sum().rename(columns={'Deaths':'Décès totaux'}))
print('-----------------------------------------')

#BRAVO! Vous avez scrapé et traité des données en utilisant Pandas.

#PARTIE B
# Essayons maintenant avec Requests et Beautiful Soup
url = 'https://fr.wikipedia.org/wiki/Didier_Raoult'

# 1: Importez la bibliothèque Requests et Beautiful Soup
import requests
import bs4

# 2: Scrapez la page web à l'url donnée
response = requests.get('https://fr.wikipedia.org/wiki/Didier_Raoult')
soup = bs4.BeautifulSoup(response.text, 'html.parser')

# 3: Trouver le titre de la page, en utilisant Beautiful Soup bien sûr ;)
soup.title.text

# 4: Combien y-a-t-il de section balisée <a> dans cette page?
len(soup.find_all('a'))

# 5: Interessons-nous aux listes. 
# a: Quelle balise html indique un élément d'une liste? <li></li>
# b: Combien y-a-t-il de ces balises dans la page web en question? len(soup.find_all('li')) => 901
# c: scrapez ces éléments
list_li = soup.find_all('li')
print(list_li)

# 6 BONUS: Nous souhaitons extraire la liste des prix obtenus par le Professeur Raoult
# a: à partir de la liste obtenue lors de la question 5, cherchez les éléments correspondant à la liste des prix et récompenses.
# quels sont les index de ces éléments?
index_prix = [82,83,84,85,86]
# c: extraire ces éléments et les mettre dans une nouvelle variable
#c = [a[index] for index in b]
prices = [list_li[index] for index in index_prix]
prices
# d: extraire uniquement le texte afin d'obtenir une jolie liste des prix et récompenses
for price in prices:
    print(price.text)


#BRAVISSIMO!!!!




