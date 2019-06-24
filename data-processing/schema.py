from pyspark.sql.types import *

SCHEMA = {}

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

SCHEMA['user_schema'] = users_schema

# Tags.xml
tags_schema = StructType([
    StructField('_Count', LongType()),
    StructField('_ExcerptPostId', LongType()),
    StructField('_Id', LongType()),
    StructField('_TagName', StringType()),
    StructField('_WikiPostId', LongType())
    ])
SCHEMA['tags_schema'] = tags_schema

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
SCHEMA['posts_schema'] = posts_schema

# Badges.xml
badges_schema = StructType([
    StructField('_Class', LongType()),
    StructField('_Date', StringType()),
    StructField('_Id', LongType()),
    StructField('_Name', StringType()),
    StructField('_TagBased', BooleanType()),
    StructField('_UserId', LongType())
    ])
SCHEMA['badges_schema'] = badges_schema

# Votes.xml
votes_schema = StructType([
    StructField('_CreationDate', StringType()),
    StructField('_Id', LongType()),
    StructField('_PostId', LongType()),
    StructField('_VoteTypeId', LongType())
    ])
SCHEMA['votes_schema'] = votes_schema

