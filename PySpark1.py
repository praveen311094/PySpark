# Databricks notebook source
# PYSPARK IS CASE-SENSITIVE LIKE PYTHON
# import pyspark.sql.functions as f
# df = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/first_test.csv")
#df1 = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/first_test.csv")
# df.show() # to view data
# df.printSchema() # column and data types
# df.select("id","name").show()
# df.filter(df.id>3).show()
# df.select(df.name,df.zip+1).show()
# df.groupby("zip").sum("id").show()
# df.agg({"id":"max"}).show()
# df.agg({"id":"count"}).show()
# df.agg({"id":"avg"}).show()
# print(df.columns) # column names
# df.filter((df.id==1) & (df.zip==560098)).show() AND
# df.filter((df.id==1) | (df.zip==560098)).show() OR
# rdd1 = sc.parallelize([("Praveen",28),("Rancho",29),("Nidhi",25)])
# res2 = rdd1.toDF(["Name","Age"]) # to create headers
# res2.show()
# new_df = df.where(df.id>2).select("id","zip")
# df.show()
# df = df.drop("_c4") # dropped column _c4
# df.show()
# uni_res = df1.union(df)  #Union
# uni_res.show()
# inter_res = df1.intersect(df)
# inter_res.show()
# df1.orderBy(f.asc("zip")).show() # Ascending
# df1.orderBy(f.desc("zip")).show() # Descending


# COMMAND ----------

# DBTITLE 1,Display
#df3 = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/bank_mrktng_cleaned_data.csv")
display(df3)
