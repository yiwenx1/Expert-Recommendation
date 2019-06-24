from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, regexp_replace, when

def load_xml():
    # Users.xml
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

    users_df = spark.read.format('xml').options(rowTag='row').schema(users_schema).load('s3a://stackoverflow-full-insight/Users.xml')
    # users_df.show()
    # users_df.printSchema()
    
    # Tags.xml
    tags_schema = StructType([
        StructField('_Count', LongType()),
        StructField('_ExcerptPostId', LongType()),
        StructField('_Id', LongType()),
        StructField('_TagName', StringType()),
        StructField('_WikiPostId', LongType())
        ])

    tags_df = spark.read.format('xml').options(rowTag='row').schema(tags_schema).load('s3a://stackoverflow-full-insight/Tags.xml')
    # tags_df.show()
    # tags_df.printSchema()

    # Posts.xml
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

    # Badges.xml
    badges_schema = StructType([
        StructField('_Class', LongType()),
        StructField('_Date', StringType()),
        StructField('_Id', LongType()),
        StructField('_Name', StringType()),
        StructField('_TagBased', BooleanType()),
        StructField('_UserId', LongType())
        ])

    badges_df = spark.read.format('xml').options(rowTag='row').schema(badges_schema).load('s3a://stackoverflow-full-insight/Badges.xml')
    badges_df.show()
    # badges_df.printSchema()

    # Votes.xml
    votes_schema = StructType([
        StructField('_CreationDate', StringType()),
        StructField('_Id', LongType()),
        StructField('_PostId', LongType()),
        StructField('_VoteTypeId', LongType())
        ])

    votes_df = spark.read.format('xml').options(rowTag='row').schema(votes_schema).load('s3a://stackoverflow-full-insight/Votes.xml')
    votes_df.show()
    # votes_df.printSchema()

    return posts_df, votes_df, badges_df, users_df, tags_df

def process_posts(df):
    # Post.Id | Post.OwnerUserId | Tag
    df = df.select('_Id', '_OwnerUserId', '_Tags').na.drop()
    df = df.withColumn('_Tags', explode(split('_Tags', '(?=<)|(?<=>)')))
    df = df.withColumn('_Tags', regexp_replace('_Tags', r'\<|\>', ''))
    # df.write.parquet('s3a://stackoverflow-full-insight/DataFrame/Post_User_Tag.parquet', mode='overwrite')
    return df

def process_badges(df):
    # df.filter(df._UserId==1677) 
    # df.show()
    """
    df.createOrReplaceTempView("Badges")
    sqlDF = spark.sql('''
            SELECT _UserId, _Class
            FROM Badges
            WHERE _UserId = 2927
            ''')
    sqlDF.show(100)
    """

    df.createOrReplaceTempView("Badges")
    sqlDF = spark.sql('''
            SELECT _UserId, SUM(
                CASE 
                    WHEN _Class=1 THEN 5
                    WHEN _CLass=2 THEN 3
                    WHEN _Class=3 THEN 1
                    ELSE 0
                END) AS BadgeScore
            FROM Badges
            GROUP BY _UserId
            ''')
    sqlDF.show()
    # df.select('_UserId', when(df['_Class'] == 1, 5).when(df['_Class'] == 2, 3).when(df['_Class'] == 3, 1).otherwise(0).alias('BadgeScore')).show()
    return sqlDF

def process_upvotes(votes_df, post_user_tag):
    votes_df.createOrReplaceTempView("Votes")
    post_user_tag.createOrReplaceTempView("Post_User_Tag")
    sqlDF = spark.sql('''
            SELECT Post_User_Tag._Tags, Post_User_Tag._OwnerUserId, SUM(
                                    CASE 
                                        WHEN Votes._VoteTypeId = 2 THEN 1
                                        ELSE 0
                                    END) AS Upvotes
            FROM Votes JOIN Post_User_Tag ON Votes._Id = Post_User_Tag._OwnerUserId
            GROUP BY Post_User_Tag._Tags, Post_User_Tag._OwnerUserId
           ''' )
    sqlDF.show()
    # df.write.parquet('s3a://stackoverflow-full-insight/DataFrame/Upvotes.parquet', mode='overwrite')
    return sqlDF

def process_allScore(user_badgeScore, users_df, user_upvotes):
    user_badgeScore.createOrReplaceTempView("BadgeScore")
    users_df.createOrReplaceTempView("Users")
    user_upvotes.createOrReplaceTempView("Upvotes")
    sqlDF = spark.sql('''
            SELECT tbl2.UserId, (tbl2.BadgeScore + tbl2._Reputation + tbl2.Upvotes) AS AllScore
            FROM (BadgeScore b JOIN Users u on b._UserId = u._Id) AS tbl1 JOIN Upvotes v ON tbl1._UserId = v._OwnerUserId AS tbl2
            ''')
    sqlDF.show()
    # df.write.parquet('s3a://stackoverflow-full-insight/DataFrame/AllScore.parquet', mode='overwrite')

def database(df):
    pass

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Load Data").getOrCreate()
    posts_df, votes_df, badges_df, users_df, tags_df = load_xml()

    post_user_tag = process_posts(posts_df)
    post_user_tag.show()
    # +---+------------+-------------------+
    # |_Id|_OwnerUserId|              _Tags|
    # +---+------------+-------------------+
    # |  4|           8|                 c#|
    # |  4|           8|     floating-point|
    # +---+------------+-------------------+

    user_badgeScore = process_badges(badges_df)
    user_badgeScore.show()
    # +-------+----------+
    # |_UserId|BadgeScore|
    # +-------+----------+
    # |   1677|        35|
    # |   3764|       204|
    # +-------+----------+

    user_upvotes = process_upvotes(votes_df, post_user_tag)

    process_allScore(user_badgeScore, users_df, user_upvotes)
