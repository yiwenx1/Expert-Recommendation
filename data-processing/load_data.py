from pyspark.sql import SparkSession
from pyspark.sql.types import *

def load_xml():
    users_schema = StructType([
        StructField('_AboutMe', StringType()),
        StructField('_AccountId', LongType()),
        StructField('_CreationDate', StringType()),
        StructField('_DisplayName', StringType()),
        StructField('_DownVotes', LongType()),
        StructField('_Id', LongType()),
        StructField('_LastAccessDate', StringType()),
        StructField('_Location', StringType()),
        StructField('_ProfileImageUrl', StringType()),
        StructField('_Reputation', LongType()),
        StructField('_UpVotes', LongType()),
        StructField('_Views', LongType()),
        StructField('_WebsiteUrl', StringType())
        ])

    # users_df = spark.read.format('xml').options(rowTag='row').schema(users_schema).load('s3a://stackoverflow-full-insight/Users.xml')
    # users_df.show()
    # users_df.printSchema()

    tags_schema = StructType([
        StructField('_Count', LongType()),
        StructField('_ExcerptPostId', LongType()),
        StructField('_Id', LongType()),
        StructField('_TagName', StringType()),
        StructField('_WikiPostId', LongType())
        ])

    # tags_df = spark.read.format('xml').options(rowTag='row').schema(tags_schema).load('s3a://stackoverflow-full-insight/Tags.xml')
    # tags_df.show()
    # tags_df.printSchema()

    posts_schema = StructType([
        StructField('_AcceptedAnswerId', LongType()),
        StructField('_AnswerCount', LongType()),
        StructField('_Body', StringType()),
        StructField('_CommentCount', LongType()),
        StructField('_CommunityOwnedDate', StringType()),
        StructField('_CreationDate', StringType()),
        StructField('_FavoriteCount', LongType()),
        StructField('_Id', LongType()),
        StructField('_LastActivityDate', StringType()),
        StructField('_LastEditDate', StringType()),
        StructField('_LastEditorUserId', LongType()),
        StructField('_LastEditorDisplayName', StringType()),
        StructField('_OwnerUserId', LongType()),
        StructField('_ParentId', LongType()),
        StructField('_PostTypeId', LongType()),
        StructField('_Score', LongType()),
        StructField('_Tags', StringType()),
        StructField('_Title', StringType()),
        StructField('_ViewCount', LongType())
        ])

    posts_df = spark.read.format('xml').options(rowTag='row').schema(posts_schema).load('s3a://stackoverflow-full-insight/Posts.xml')
    # posts_df.show()
    # posts_df.printSchema()
    return posts_df

def process_posts(df):
    df.filter(df['_Tags'] != None).select(df['_Tags']).show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Load Data").getOrCreate()
    posts_df = load_xml()
    process_posts(posts_df)
   
