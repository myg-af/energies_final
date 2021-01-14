#Quizz scrapping

#Q1: Quelles librairies Python nous permmettent de scrapper des données sur les pages web? (indice: tapez "scraping python" dans Google)

#Q2: Une librairie que nous avons déjà utilisée en début de formation nous permet également de scraper des données sur une page web. Laquelle?

#Q3: 
# a: Que fait la fonction .read_html de Pandas?
# b: Quels éléments la fonction recherche-t-elle dans un document html?
# c: Quel type d'objet la fonction renvoit-elle?

#Q4: La librairie Requests
# a: À quoi sert cette librairie?
# b: Que permet-elle d'obtenir dans une action de scraping de page web?

#Q5: La librairie Beautiful Soup
# a: À quoi sert cette librairie?
# b: Peut-elle être utilisée seule pour scraper une page web?
# c: Quel est son intérêt par rapport à Requests?

#À votre tour de jouer! Nous allons acquérir ensemble des données brutes, les travailler, les nettoyer.

#Voici la premère page web visée.
url = 'https://coronavirus.jhu.edu/data/mortality'
#Entrz l'url dans votre navigateur web et observer la page.
#Nous souhaitons scrapper le tableau contenant les données sur le coronavirus dans le monde
# 
# 1: scrapper le tableau de données. Quelle méthode allez-vous utiliser?
# 
# 2: Maintenant que vous avez ce tableau, nous souhaitons avoir un tableau des 10 pays ayant eu le plus de morts ("Deaths").
# Enregister ce dataFrame dans une variable
# 
# 3: Remplacer le nom des pays par le continent. Supprimer les colonnes inutilisées.
# Asie, Europe, Amerique du Nord, Amerique du Sud, Afrique, Antartique
# Exemple => dic = {'US':'Amerique du Nord','Brazil':'Amerique du Sud','Mexico':'Amerique du Nord','India':'Asie','United Kingdom':'Europe','Italy':'Europe','France':'Europe','Spain':'Europe','Peru':'Amerique du Sud','Iran':'Asie'}
# 
# 4: Obtenir une somme par continent du nombre de mort. Enregistrer ce tableau dans une nouvelle variable.

#BRAVO! Vous avez scrapé et traité des données en utilisant Pandas.

#-----------------------------------------------------------------------------------------------------------------------

#PARTIE B
# Essayons maintenant avec Requests et Beautiful Soup
url = 'https://fr.wikipedia.org/wiki/Didier_Raoult'

# 1: Importez la bibliothèque Requests et Beautiful Soup

# 2: Scrapez la page web à l'url donnée

# 3: Trouver le titre de la page, en utilisant Beautiful Soup bien sûr ;)

# 4: Combien y-a-t-il de section balisée <a> dans cette page?

# 5: Interessons-nous aux listes. 
# a: Quelle balise html indique un élément d'une liste? 
# b: Combien y-a-t-il de ces balises dans la page web en question?
# c: scrapez ces éléments

# 6 BONUS: Nous souhaitons extraire la liste des prix obtenus par le Professeur Raoult
# a: à partir de la liste obtenue lors de la question 5, cherchez les éléments correspondant à la liste des prix et récompenses.
# b: quels sont les index de ces éléments?
# c: extraire ces éléments et les mettre dans une nouvelle variable
# d: extraire uniquement le texte afin d'obtenir une jolie liste des prix et récompenses

#BRAVISSIMO!!!!



