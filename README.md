# TP - Spark

Lien vers le GitHub : [TP Spark](https://github.com/andrewarnaud1/tp-spark)

Participants :
Andrew Arnaud
Mohamed Diallo
Souleimane Hadbi

---

## Section 1.3 : Ingestion des données en flux

### Création de répertoires pour les données en flux

- **Commentez le résultat (faites une recherche).**

L'exécution de la commande `dbutils.fs.mkdirs("dbfs:/FileStore/tables/stream_read")` renvoie `True`, ce qui indique que le répertoire `stream_read` a été créé avec succès dans le système de fichiers DBFS (Databricks File System). La fonction `dbutils.fs.mkdirs` est utilisée pour créer des répertoires dans DBFS, et le retour de `True` confirme que l'opération s'est terminée sans erreur.

![Création répertoire](https://github.com/andrewarnaud1/tp-spark/blob/main/1.png?raw=true)

- **Vérifiez la bonne création du répertoire (hint : Il faut vérifier depuis le catalogue).**

La bonne création du répertoire `stream_read` a été vérifiée à l'aide du catalogue. La capture d'écran montre que le répertoire `stream_read` est bien présent dans le chemin spécifié.

![Vérification répertoire](https://github.com/andrewarnaud1/tp-spark/blob/main/2.png?raw=true)

## 1.4 : Premier pas en stream

### Préparation du schéma des données

- **Expliquez cette instruction. À votre avis, d'où vient cette structure ?**

L'instruction `schema_defined = StructType([...])` définit un schéma pour les données de flux en utilisant la classe `StructType` de PySpark. Chaque `StructField` dans la liste spécifie le nom de la colonne, le type de données (comme `LongType` ou `StringType`), et un indicateur de nullabilité (`True` signifie que la colonne peut contenir des valeurs nulles). Cette structure semble être basée sur un dataset préexistant ou une documentation qui décrit la structure des données à ingérer. Elle correspond à un dataset typique utilisé pour les tâches de classification ou d'analyse, tel qu'un dataset de revenus ou de recensement.

![Structure schéma](https://github.com/andrewarnaud1/tp-spark/blob/main/3.png?raw=true)

### Lecture du flux de données depuis le répertoire

- **Essayez de commenter chaque option de l'instruction**

  ```python
  df = (spark.readStream
        .format("csv")              # Spécifie le format des données d'entrée comme CSV.
        .schema(schema_defined)     # Utilise le schéma défini précédemment pour structurer les données.
        .option("header", True)     # Indique que le fichier CSV contient une ligne d'en-tête avec les noms de colonnes.
        .option("sep", ",")         # Spécifie que les colonnes du CSV sont séparées par des virgules.
        .load("/Volumes/iut_approfondissement/default/vol-iut/FileStore/tables/stream_read/"))  # Spécifie le chemin du répertoire contenant les fichiers CSV à lire en flux.
  ```

![Lecture du flux](https://github.com/andrewarnaud1/tp-spark/blob/main/4.png?raw=true)

### Requêtes sur les données

- **Dans une nouvelle cellule, nous souhaitons voir en agrégé combien d'hommes et de femmes nous avons. Complétez et exécutez :**

  ```python
  df_grouped_sex = df.groupBy("sex").count()
  ```

![Agrégation sexe](https://github.com/andrewarnaud1/tp-spark/blob/main/5.png?raw=true)

- **Dans une nouvelle cellule, nous souhaitons voir en agrégé la répartition des classes de travailleurs (hint : workclass). Complétez et exécutez :**

  ```python
  df_grouped_workclass = df.groupBy("workclass").count()
  ```

![Agrégation travailleurs](https://github.com/andrewarnaud1/tp-spark/blob/main/6.png?raw=true)

### Affichage des résultats en flux (La magie de Spark)

- **Commentez le résultat**

L'exécution de la commande `display(df_grouped_workclass)` renvoie "Query returned no results", ce qui signifie qu'il n'y a actuellement aucune donnée dans le DataFrame `df_grouped_workclass` à afficher. Cela pourrait être dû au fait qu'aucun fichier de données n'a encore été ajouté au répertoire surveillé ou que les données n'ont pas été ingérées correctement.

![Résultat agrégation travailleurs](https://github.com/andrewarnaud1/tp-spark/blob/main/7.png?raw=true)

### Ingestion des données en flux

- **Que constatez-vous dans l'autre fenêtre ?**

Après avoir ajouté le fichier `adult1.csv`, la fenêtre affichant le résultat de la commande `display(df_grouped_workclass)` devrait montrer des résultats agrégés des classes de travailleurs. Les données de `adult1.csv` sont ingérées et traitées en temps réel, ce qui permet d'afficher les comptes de chaque classe de travailleurs dans le tableau.

![Résultat agrégation travailleurs](https://github.com/andrewarnaud1/tp-spark/blob/main/8.png?raw=true)

Ajoutez depuis la fenêtre de l'ingestion des données le fichier `adult2.csv`.

- **Que constatez-vous dans l'autre fenêtre ?**

Avec l'ajout du fichier `adult2.csv`, le tableau se met à jour pour refléter les nouvelles données ingérées.

Ajoutez maintenant dans la cellule du `display(df_grouped_workclass)` une visualisation, en cliquant sur (+) à côté de tables.

Ajoutez depuis la fenêtre de l'ingestion des données le fichier `adult3.csv`.

- **Que constatez-vous dans l'autre fenêtre avec la visualisation ? (capture d'écran)**

Avec l'ajout du fichier `adult3.csv`, la visualisation (graphique en barres) se met à jour pour montrer la répartition des classes de travailleurs de manière visuelle. Cette mise à jour en temps réel permet de voir comment les nouvelles données influencent la distribution des classes de travailleurs.

![Visualisation](https://github.com/andrewarnaud1/tp-spark/blob/main/9.png?raw=true)

- **Que constatez-vous dans l'autre fenêtre ?**

Après avoir ajouté les fichiers `adult4.csv`, `adult5.csv`, et `adult6.csv`, la fenêtre affichant le résultat de la commande `display(df_grouped_sex)` montre maintenant une agrégation des données par genre. Le tableau présente le nombre total d'hommes et de femmes dans les fichiers ingérés, et les données se mettent à jour en temps réel pour refléter les nouvelles données ajoutées.

![Résultat agrégation genre](https://github.com/andrewarnaud1/tp-spark/blob/main/10.png?raw=true)

- **Visualisation**

Ensuite, ajoutez une visualisation pour représenter graphiquement la répartition des genres en cliquant sur (+) à côté de tables dans la cellule `display(df_grouped_sex)`.

![Visualisation genre](https://github.com/andrewarnaud1/tp-spark/blob/main/11.png?raw=true)

---

## Section 1.4 : Persistance des données en flux dans des fichiers

### Création du répertoire checkpoint

- **Comment avez-vous procédé ?**

Nous avons utilisé la commande `dbutils.fs.mkdirs` pour créer le répertoire `stream_checkpoint` au même niveau que `stream_read` et `stream_write`. Cette commande crée le répertoire spécifié dans le Databricks File System (DBFS).

```python
dbutils.fs.mkdirs("dbfs:/Volumes/iut_approfondissement_4362529416329370/default/vol-iut/FileStore/tables/stream_checkpoint")
```

Le retour `True` indique que le répertoire a été créé avec succès.

![Création du répertoire checkpoint](https://github.com/andrewarnaud1/tp-spark/blob/main/12.png?raw=true)

### Persister le flux de données

- **Commande pour persister les données en flux**

La commande suivante persiste les données en flux dans des fichiers Parquet, en utilisant `writeStream` de Spark.

```python
df_persisted = df.writeStream.format("parquet") \
                .outputMode("append") \
                .option("path", "/Volumes/iut_approfondissement_4362529416329370/default/vol-iut/FileStore/tables/stream_write/") \
                .option("checkpointLocation", "/Volumes/iut_approfondissement_4362529416329370/default/vol-iut/FileStore/tables/stream_checkpoint/") \
                .start() \
                .awaitTermination()
```

![Persistance du flux](https://github.com/andrewarnaud1/tp-spark/blob/main/13.png?raw=true)

### Vérification du contenu des répertoires `stream_checkpoint` et `stream_write`

- **Vérifiez le contenu après ajout des fichiers `adult7.csv` et `adult8.csv`**

Après l'ajout des fichiers `adult7.csv` et `adult8.csv` au répertoire `stream_read`, le contenu des répertoires `stream_checkpoint` et `stream_write` doit être mis à jour.

- **Contenu du répertoire `stream_checkpoint`**

Le répertoire `stream_checkpoint` contient des dossiers et des fichiers de métadonnées nécessaires pour maintenir l'état du flux de données.

![Contenu du répertoire checkpoint](https://github.com/andrewarnaud1/tp-spark/blob/main/14.png?raw=true)

- **Contenu du répertoire `stream_write`**

Le répertoire `stream_write` contient les fichiers Parquet avec les données persistées depuis le flux.

![Contenu du répertoire stream_write](https://github.com/andrewarnaud1/tp-spark/blob/main/15.png?raw=true)

### Vérification de la persistance des fichiers

Pour vérifier que les fichiers ont bien été persistés, utilisez la commande suivante pour afficher le contenu des fichiers Parquet dans le répertoire `stream_write` :

```python
display(spark.read.format("parquet").load("/Volumes/iut_approfondissement_4362529416329370/default/vol-iut/FileStore/tables/stream_write/*.parquet"))
```

![Vérification des fichiers persistés](https://github.com/andrewarnaud1/tp-spark/blob/main/16.png?raw=true)

### Du flux à la base de données

Le flux ne fonctionne pas. nous n'avons pas réussi à le mettre en place. Lorsque l'opération démarre elle ne finis pas sont traitement :

![Flux base de données](https://github.com/andrewarnaud1/tp-spark/blob/main/17.png?raw=true)

---

# Stream des données depuis une API

### Création de l’API et import des données dans un DataFrame

```python
import requests
import pandas as pd

def fetch_and_store_data(api_key):
    # Définir l'URL de l'API avec la clé API
    url = f"https://api.jcdecaux.com/vls/v1/stations?apiKey={api_key}"

    # Faire une requête GET à l'API
    response = requests.get(url)

    # Vérifier si le statut de la réponse est valide (200)
    if response.status_code == 200:
        # Obtenir la réponse JSON
        data = response.json()

        # Convertir la liste de dictionnaires en DataFrame
        df = pd.DataFrame(data)

        # Transformer les colonnes imbriquées (par exemple, 'position') en colonnes séparées
        if 'position' in df.columns:
            df_position = df['position'].apply(pd.Series)
            df = pd.concat([df, df_position], axis=1).drop(columns=['position'])

        # Convertir les timestamps Unix en datetime
        if 'last_update' in df.columns:
            df['last_update'] = pd.to_datetime(df['last_update'], unit='ms')

        return df
    else:
        print("Erreur: Problème lors de la récupération des données de l'API JCDecaux")
        return None

api_key = "b847e3c5c3542929a06db3d95cf30867b91dc4d1"
df = fetch_and_store_data(api_key)

if df is not None:
    # Vérifier les valeurs manquantes
    print(df.isnull().sum())

    # Afficher les premières lignes du DataFrame
    print(df.head())
    print(df.columns)
```

### Création d’une carte avec folium

```python
import folium
from folium.plugins import MarkerCluster

# Créer une carte centrée sur les coordonnées moyennes des stations
map_center = [df['lat'].mean(), df['lng'].mean()]
mymap = folium.Map(location=map_center, zoom_start=6)

# Ajouter des marqueurs pour chaque station
marker_cluster = MarkerCluster().add_to(mymap)
for idx, row in df.iterrows():
    folium.Marker([row['lat'], row['lng']], 
                  popup=f"Station: {row['name']}\nVélos disponibles: {row['available_bikes']}").add_to(marker_cluster)

# Afficher la carte
mymap
```

Nous obtenons une carte avec des points. Lorsque nous cliquons sur un point, nous voyons le détail de la station (son nom et le nombre de vélos disponibles).

![Carte folium](https://github.com/andrewarnaud1/tp-spark/blob/main/18.png?raw=true)

### Autres visualisations

```python
import plotly.express as px
# Histogramme de la disponibilité des vélos
fig = px.histogram(df, x='available_bikes', nbins=20, title='Distribution de la disponibilité des vélos')
fig.update_layout(xaxis_title='Nombre de vélos disponibles', yaxis_title='Nombre de stations')
fig.show()
```

![Histogramme](https://github.com/andrewarnaud1/tp-spark/blob/main/19.png?raw=true)

```python
# Calculer la disponibilité moyenne des vélos par contrat (ville)
city_bike_availability = df.groupby('contract_name')['available_bikes'].mean().reset_index()

# Carte de chaleur
fig = px.bar(city_bike_availability, x='contract_name', y='available_bikes', title='Disponibilité moyenne des vélos par ville')
fig.update_layout(xaxis_title='Ville', yaxis_title='Disponibilité moyenne des vélos')
fig.show()
```

![Disponibilité moyenne](https://github.com/andrewarnaud1/tp-spark/blob/main/20.png?raw=true)

```python
# Diagramme de barres de la disponibilité des vélos pour les premières stations
top_n = 10  # Nombre de premières stations à afficher
df_top_n = df.head(top_n)

fig = px.bar(df_top_n, x='name', y='available_bikes', title=f'Disponibilité des vélos pour les {top_n} premières stations')
fig.update_layout(xaxis_title='Station', yaxis_title='Nombre de vélos disponibles', xaxis_tickangle=-45)
fig.show()
```

![Diagramme](https://github.com/andrewarnaud1/tp-spark/blob/main/21.png?raw=true)

```python
# Compter les stations ouvertes et fermées
status_counts = df['status'].value_counts().reset_index()
status_counts.columns = ['status', 'count']

fig = px.bar(status_counts, x='status', y='count', title='Nombre de stations ouvertes et fermées')
fig.update_layout(xaxis_title='Statut de la station', yaxis_title='Nombre de stations')
fig.show()
```

![Somme stations ouvertes fermées](https://github.com/andrewarnaud1/tp-spark/blob/main/22.png?raw=true)

```python
fig = px.scatter(df, x='lat', y='lng', color='contract_name', size='available_bikes', hover_name='name',
                 title='Disponibilité des vélos par ville')
fig.update_layout(xaxis_title='Latitude', yaxis_title='Longitude')
fig.show()
```

![Scatter plot](https://github.com/andrewarnaud1/tp-spark/blob/main/23.png?raw=true)
