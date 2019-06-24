from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import explode, split, regexp_replace
import re

spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
sc = spark.sparkContext
df = spark.createDataFrame([("<#spark><java>", 1), ("<ab.c><p++ython><p.hp>", 2), ("<a>", 3)], ["Tags", "UserId"])
df.show()
df = df.withColumn("Tags", explode(split("Tags", "(?=<)|(?<=>)")))
df = df.withColumn("Tags", regexp_replace("Tags", r'\<|\>', ""))
df.filter(df.Tags != "").show()

def split_tags(row):
    owner_user_id = row.UserId
    tags = row.Tags
    result = re.findall("<(.*?)>", tags)
    list = []
    for res in result:
        list.append([owner_user_id, res])
    print(list)
    return spark.parallelize(list)

# df.rdd.map(split_tags).toDF().show()
