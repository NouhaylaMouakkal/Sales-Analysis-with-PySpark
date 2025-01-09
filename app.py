from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, rank, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.types import DateType

# Initialiser une session Spark
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

# 1. Lire le fichier CSV
df = spark.read.csv("./data/sales_data.csv", header=True, inferSchema=True)
df.show()

# 2. Vérifier le schéma des données et compter le nombre de lignes
df.printSchema()
print("Nombre de lignes:", df.count())

# 3. Filtrer les transactions avec un montant supérieur à 100
df_filtered = df.filter(col("amount") > 100)
df_filtered.show(5)

# 4. Remplacer les valeurs nulles dans les colonnes amount et category par des valeurs par défaut
df = df.fillna({"amount": 0.0, "category": "Default"})
df.show(5)

# 5. Convertir la colonne date en un format de date pour les analyses temporelles
df = df.withColumn("date", col("date").cast(DateType()))

# 6. Calculer le total des ventes pour l'ensemble de la période
total_sales = df.agg(sum("amount").alias("total_sales")).collect()[0]["total_sales"]
print("Total des ventes pour la période:", total_sales)

# 7. Calculer le montant total des ventes par catégorie de produit
sales_by_category = df.groupBy("category").agg(sum("amount").alias("total_sales"))
sales_by_category.show()

# 8. Calculer le montant total des ventes par mois
sales_by_month = df.withColumn("month", month(col("date"))) \
                   .groupBy("month") \
                   .agg(sum("amount").alias("monthly_sales"))
sales_by_month.show()

# 9. Identifier les 5 produits les plus vendus en termes de montant total
top_5_products = df.groupBy("product_id") \
                   .agg(sum("amount").alias("total_sales")) \
                   .orderBy(col("total_sales").desc()) \
                   .limit(5)
top_5_products.show()

# 10. Pour chaque catégorie, trouver le produit le plus vendu

sales_by_product_category = df.groupBy("category", "product_id") \
                              .agg(sum("amount").alias("total_sales"))

# fonction de fenêtre pour classer les produits par total des ventes dans chaque catégorie

window_spec = Window.partitionBy("category").orderBy(col("total_sales").desc())
top_product_by_category = sales_by_product_category \
    .withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("category", "product_id", "total_sales")

top_product_by_category.show()