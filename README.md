# DEMO : Analyse des Ventes avec PySpark

## Contexte
Ce demo-projet utilise **PySpark** pour analyser les données de vente d'une entreprise de commerce en ligne. Les données de vente incluent des informations sur chaque transaction, telles que l'ID du produit, le montant de la vente, et la catégorie de produit. Le but est de répondre à plusieurs questions d’analyse de vente en effectuant des opérations de filtrage, d’agrégation et d’identification des produits les plus vendus.

## Fichiers inclus

- **sales_data.csv** : Le fichier de données de ventes utilisé dans l'analyse.
  - Exemple de contenu :
    ```plaintext
    transaction_id,client_id,product_id,date,amount,category
    1,101,201,2023-01-15,150,Electronics
    2,102,202,2023-01-18,200,Home
    3,103,203,2023-01-20,50,Toys
    ```
- **app.py** : Script PySpark contenant le code de l’analyse.
- **docker-compose.yml** : Fichier Docker Compose pour configurer et lancer les services Spark Master et Spark Workers.

## Objectifs et Résultats
Les objectifs et résultats obtenus pour chaque question sont détaillés ci-dessous.

### 1. Charger et explorer les données
- **Objectif** : Charger le fichier CSV et afficher les premières lignes.
- **Résultat** : Les premières lignes des données sont affichées, montrant les informations de chaque transaction.

### 2. Vérifier le schéma et compter le nombre de lignes
- **Résultat** :
  ![alt text](img/image.png)

### 3. Filtrer les transactions avec un montant supérieur à 100 dhs
- **Résultat** :
![alt text](img/image-1.png)

### 4. Remplacer les valeurs nulles dans les colonnes `amount` et `category`
- **Objectif** : Remplacer les valeurs nulles par des valeurs par défaut (0.0 pour `amount` et "Default" pour `category`).
- **Résultat** :
![alt text](img/image-3.png)

### 5. Convertir la colonne `date` en format date
- **Objectif** : Convertir la colonne `date` en type date pour des analyses temporelles.
- **Résultat** : La colonne `date` est convertie en format date. Les valeurs sont maintenant au format `yyyy-MM-dd`.

### 6. Calculer le total des ventes pour l'ensemble de la période
- **Objectif** : Calculer le montant total des ventes.
- **Résultat** :
![alt text](img/image-2.png)

### 7. Calculer le montant total des ventes par catégorie de produit
- **Objectif** : Calculer le total des ventes pour chaque catégorie.
- **Résultat** :
  ```
  +-----------+-----------+
  | category  | total_sales |
  +-----------+-----------+
  | Electronics | 910       |
  | Home        | 795       |
  | Fashion     | 1245      |
  | Toys        | 170       |
  +-----------+-----------+
  ```

### 8. Calculer le montant total des ventes par mois
- **Objectif** : Calculer le montant total des ventes pour chaque mois.
- **Résultat** :
  ```
  +-------+--------------+
  | month | monthly_sales|
  +-------+--------------+
  |   1   | 400          |
  |   2   | 505          |
  |   3   | 400          |
  |   4   | 390          |
  |   5   | 340          |
  |   6   | 230          |
  |   7   | 350          |
  |   8   | 50           |
  |   9   | 260          |
  |  10   | 110          |
  |  11   | 225          |
  +-------+--------------+
  ```

### 9. Identifier les 5 produits les plus vendus en termes de montant total
- **Objectif** : Trouver les 5 produits avec les ventes les plus élevées.
- **Résultat** :
  ```
  +----------+-----------+
  |product_id| total_sales|
  +----------+-----------+
  |       205|        300 |
  |       212|        250 |
  |       207|        220 |
  |       208|        180 |
  |       210|        130 |
  +----------+-----------+
  ```

### 10. Trouver le produit le plus vendu par catégorie
- **Objectif** : Identifier le produit avec le montant de ventes le plus élevé dans chaque catégorie.
- **Résultat** :
  ```
  +-----------+----------+-----------+
  | category  | product_id | total_sales|
  +-----------+----------+-----------+
  | Electronics | 201      | 150       |
  | Fashion     | 205      | 300       |
  | Home        | 202      | 200       |
  | Toys        | 203      | 100       |
  +-----------+----------+-----------+
  ```

## Exécution du Script
Pour exécuter le script `app.py` dans le conteneur `spark-master`, utilisez la commande suivante :

```bash
docker-compose up -d  # Démarre les conteneurs en arrière-plan
docker exec -it spark-master spark-submit /app/app.py
```

Cette commande exécutera l'analyse des ventes dans l'environnement Spark, et les résultats apparaîtront dans la console.