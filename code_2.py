import pyspark
from pyspark.sql import SparkSession
Spark = SparkSession.builder.appName("Nihala").getOrCreate()
#
# data = [("Nihala","Taskeen",22,'F'),
#         ("Nancy","Mary",22,'F'),
#         ("Lish","suruthi",22,'F')]
# columns = ["firstname","lastname","age","gender"]
# df = Spark.createDataFrame(data , columns)
# df.show()

dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])
df = Spark.createDataFrame(dataDF , schema)

df.withColumnRenamed("dob","DateOfBirth").printSchema()

df.show()
