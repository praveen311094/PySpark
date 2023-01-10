# Databricks notebook source
''''PySpark- Name indicates it is a python and spark so it very much useful to load millions data where Pyspark used RDD(Resilient Distributed dataset-component of spark) and clusters which companies love to use in real-time projects.
I have practised some main operations functions in PySpark where I have imported pyspark libraries and performed some queries on multiple "csv" files..this is the just the beginning'''

# COMMAND ----------

# PYSPARK IS CASE-SENSITIVE LIKE PYTHON
#import pyspark.sql.functions as f
#df = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/first_test.csv")
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
# df.createOrReplaceTempView("test")
# df1 = spark.sql("select name from test where id > 3 ")
# df1.show()
# display() # shows visualization on dataframe

# COMMAND ----------

# df = df.withColumn("Salary",f.lit(7500)) # Adding new column
# df.show()
# rdd1 = sc.parallelize([("rintak",10),("Rolle",13),("Rita",12),("ryink",10),("Rover",16)])
# employee = rdd1.toDF(["Name","Department_ID"])
# employee.show()

# COMMAND ----------

# employee= employee.withColumn("Emp_Sal",f.lit(7500))
# employee.show()
# employee=employee.drop('Salary')
# employee.show()
# employee = employee.withColumnRenamed("Emp_Sal","Full Salary").withColumnRenamed("Full Salary","Emp_Sal") # Rename and undo respt.
# employee.show()
# rdd1 = sc.parallelize([(10,"Sales"),(12,"Clerical"),(13,NULL),(15,"Medicine")])
# department = rdd1.toDF(["Department_ID","Dept_Name"])
# department.show()

# COMMAND ----------

#JOINS 
# employee.show()
# department.show()
#                                                                -----Inner Join ----
# inner_join = employee.join(department, "Department_ID")  
# inner_join.show()
#                                                                -----left Join ----
# left = employee.join(department, "Department_ID", "left")  
# left.show()
#                                                                -----Right Join ----
# right = employee.join(department, "Department_ID", "right")  
# right.show()
#                                                                -----Full Outer Join ----
#fullouter = employee.join(department, "Department_ID", "fullouter")
#fullouter = fullouter.show()
#                                                                -----Leftanti Join ----
# left_anti = employee.join(department, "Department_ID", "left_anti")
# left_anti.show()
#                                                                -----Leftsemi Join ----
# left_semi = employee.join(department, "Department_ID", "left_semi")
# left_semi.show()

# COMMAND ----------

# df = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/first_test.csv")
# df.show() # to view data
# df = df.drop("_c4")
# df.printSchema()
employee.show()

# COMMAND ----------

# TypeCasting
# from pyspark.sql.types import IntegerType
# employee = employee.withColumn("Department_ID",employee["Department_ID"].cast(IntegerType()))
employee.printSchema()

# COMMAND ----------

# DROPPING & REPLACING NULL VALUES

# df.show()
# df.na.drop().show() # It will remove all rows with single null also
# df = df.withColumn("_c4", df["_c4"].cast(IntegerType()))
# df.show()
# df.printSchema()
# df = df.withColumnRenamed("_c4", "other") # Rename Column
# df.show()
#sample = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/Missing_data.csv")
# sample.show()
# sample.na.fill(100).show() # it will fill for those column which includes string type(Int or Float)
# sample.na.fill("Missing Values").show() # it will missing values of string type
# sample.na.fill(100,["Name","Salary"]).show()

# COMMAND ----------

# For Multiple case statements
# from pyspark.sql.functions import when
# df = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/first_test-1.csv")
# df.show()
# df2 = df.withColumn("new_gender", when(df.gender =="M", "Male")
#                    .when(df.gender =="F", "Female")
#                    .when(df.gender.isNull(), "")
#                    .otherwise(df.gender()))

# COMMAND ----------

# For single case statement
# df2 = df.withColumn("new_gender", when(df.gender =="M", "Male")
#                    .otherwise(df.gender()))

# COMMAND ----------

#                                    ----------------DATE FUNCTIONS-----------------
#from pyspark.sql import functions as f
# df3 = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/Date.csv")
#df3.show()
# df3.printSchema()
# modifiedDF =  df3.withColumn("Date", f.to_date(df3.Date, "mm/dd/yyyy"))
# modifiedDF =  modifiedDF.withColumn("birth_year",f.year(modifiedDF.Date))
# modifiedDF =  modifiedDF.withColumn("birth_month",f.month(modifiedDF.Date))
# modifiedDF =  modifiedDF.withColumn("birth_day",f.dayofmonth(modifiedDF.Date))
# modifiedDF =  modifiedDF.withColumn("15_days_later",f.date_add(modifiedDF.Date,15))
# modifiedDF =  modifiedDF.withColumn("15_days_before",f.date_add(modifiedDF.Date,-15))
# modifiedDF.show()
modifiedDF.printSchema()

# COMMAND ----------

#df4 = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/Date_time-2.csv")
df4 = df4.withColumn("minute",f.minute(df4.Date))
df4 = df4.withColumn("hour",f.hour(df4.Date))
df4 = df4.withColumn("second",f.second(df4.Date))
df4.show()

# COMMAND ----------

#                    ------------------STARTSWITH ENDSWITH
# employee.show()
# employee.filter(employee.Name.contains("ov")).show()
# employee.filter(employee.Name.startswith("ry")).show()
# employee.filter(employee.Name.endswith("ta")).show()
# employee.filter(employee.Name.endswith("it")).show()

# COMMAND ----------

#           ---------------CONCATENTE
# df.withColumn("Concat_Example",f.concat(df.area, df.name)).show()
# df.withColumn("Concat_ws",f.concat_ws(" - ",df.area, df.name)).show() 

df.select(f.concat(df.area,df.name).alias("Full_Name")).show()  # Alias

# COMMAND ----------

#Creating dataframe without file
# data = [("James","Sales",74500),("Michael","Finance",94500),("Lia","Marketing",84500),("Stella","Finance",74500),("steve","HR",44500),("Katrina","Marketing",45100),("Sid","HR",45100)]
# columns = ["Name","Dept","Salary"]
# df5 = spark.createDataFrame(data,columns)
df5.show()
#DISTINCT
# pk= df5.select(f.countDistinct("Salary")) # one columns
# pk.show()
# pk1= df5.select(f.countDistinct("Dept","Salary"))
# pk1.show()
# pk1= df5.select(f.countDistinct("Dept"))
# pk1.show()

# COMMAND ----------

# COLLECT LIST TO GET COLUMN VALUES
pk = df5.select(f.collect_list("Salary").alias("Salary_list")).show(truncate=False)

# COMMAND ----------

#COLLECT SET -to get distinct values
pk = df5.select(f.collect_set("Salary").alias("Salary_set")).show(truncate=False)

# COMMAND ----------

# # SUBSTRING
# data =  [(1,"20230109"),(2,"20230110")]
# columns = ["id","Date"]
# df = spark.createDataFrame(data,columns)
#df.show()
# df = df.withColumn('Year',f.substring("Date", 1,4)) # args position,size
# df = df.withColumn('Month',f.substring("Date", 5,2))
# df = df.withColumn('Day',f.substring("Date", 7,2))
df.show()
df.printSchema()

# COMMAND ----------

## Slice functions in list
# pk2 = spark.createDataFrame(  [   ( [10,20,3,50,60], )  ,   ( [40,50,80,90], ) ]   ,["x"], )
# pk2.show(truncate=False)
# pk2 = spark.createDataFrame(  [   ( [10,20,3,50,60], [20,35,40,60] )  ,   ( [40,50,80,90], [50,80,90] ) ]   ,["x","y"], )
# pk2.select(f.slice(pk2.y, 1, 3).alias("Sliced_data")).show(truncate=False)
# pk2.select(f.slice(pk2.x,1,4).alias("Sliced_data1"),f.slice(pk2.y, 1, 3).alias("Sliced_data2")).show(truncate=False)

# COMMAND ----------

# Apply Function
from pyspark.sql.functions import col
# df = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/first_test.csv")
# df.show()
# def CheckValue(id):
#     if(id>3):
#         return "Greater than 3"
#     elif(id<3):
#         return "Lesser than 3"
#     else:
#         return "Equals to 3"
a = udf(CheckValue)
df_res = df.withColumn("res",a(col("id")))
df_res.show()

# COMMAND ----------

# Dictionaries 
# from pyspark.sql.functions import col
# df = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/FileStore/tables/first_test.csv")
# df.show()
df.collect()

# COMMAND ----------

dict = {}
for i in df.collect():
    dict[i["id"]]= i['name']
print(dict)

# COMMAND ----------

# def CheckValue(id):
#     return str(dict.get(id)) + "_new"
# a = udf(CheckValue) # here we r registering that function ud, it will do only for primitive and data structure data types
# df_res = df.withColumn("res",a(col("id")))
# df_res.show()
# IN UDF We can't use SQL (Use Python code)


# COMMAND ----------

df = spark.read.format("csv").option("Header","true").option("inferschema","true").load("/FileStore/tables/UIDAI_ENR_DETAIL_20170308-1.csv")
#df1 = spark.read.format("csv").option("Header","true").option("inferschema","true").load("/FileStore/tables/UIDAI_ENR_DETAIL_20170308-1.csv")
# df.printSchema()
df.show(5)

# COMMAND ----------

df = df.filter(df["Aadhaar generated"] == 0)
df1 = df.groupBy("State").count()
df2 = df1.orderBy(f.desc("count"))
df2.limit(10).show()

# COMMAND ----------

df = df.filter(df["Aadhaar generated"] == 0)
df1 = df.groupBy("Age").count()
df2 = df1.orderBy(f.desc("count"))
df2.limit(10).show()

# COMMAND ----------

# df2 = df.filter(df["Aadhaar generated"] > 0)
# df3 = df2.groupBy("Sub District","Gender").sum("Aadhaar generated")
# df3.show(5)

# get_male = df3.filter(df3["Gender"] =="M")

get_male = get_male.orderBy(f.desc("sum(Aadhaar generated)")).limit(10)
get_male.show()

# COMMAND ----------

get_female = df3.filter(df3["Gender"] =="F")

get_female = get_male.orderBy(f.desc("sum(Aadhaar generated)")).limit(10)
get_female.show()

# COMMAND ----------

data = [("James","Sales",74500),("Michael","Finance",94500),("Lia","Marketing",84500),("Stella","Finance",74500),("steve","HR",44500),("Katrina","Marketing",84500),("Sid","HR",45100)]
columns = ["Name","Dept","Salary"]
df = spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

WINDOWING FUNCTIONS
->row number-> sequential row number
->rank: leaves gaps in rank when there are ties
->dense rank: partition without any gaps

# COMMAND ----------

'''row number window function
row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition'''

# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
windowSpec = Window.partitionBy("Dept").orderBy("Salary")
res = df.withColumn("row_number",row_number().over(windowSpec))
res.show()

# COMMAND ----------

'''rank() functn is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties'''
from pyspark.sql.functions import rank
res1 = df.withColumn("rank",rank().over(windowSpec))
res1.show()

# COMMAND ----------

'''dense_rank() window function is used to get the result with rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties'''
from pyspark.sql.functions import dense_rank
res2 = df.withColumn("dense_rank",dense_rank().over(windowSpec))
res2.show()

# COMMAND ----------

# LAG FUNCTION
from pyspark.sql.functions import lag
df.withColumn("lag",lag("Salary",1).over(windowSpec)).show()

# COMMAND ----------

# LEAD FUNCTION
from pyspark.sql.functions import lead
df.withColumn("lead",lead("Salary",1).over(windowSpec)).show()

# COMMAND ----------


