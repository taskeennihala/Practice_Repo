import pyspark
from pyspark.sql import SparkSession
Spark = SparkSession.builder.appName("Nihala").getOrCreate()

data = [("Nihala","Taskeen",22,'F'),
        ("Nancy","Mary",22,'F'),
        ("Lish","suruthi",22,'F')]
columns = ["firstname","lastname","age","gender"]
df = Spark.createDataFrame(data , columns)
df.show()