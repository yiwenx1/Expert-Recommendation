# Distributed Processing

## Data

| File     | Content                                                      |
| -------- | ------------------------------------------------------------ |
| Badges   | Id, UserID, Name, Date, Class (1->gold, 2->silver, 3->bronze) |
| Comments | Id, PostId, Score, Text, CreationDate, UserID                |
| Posts    | Id, PostTypeId (1->Question, 2->Answer), AcceptedAnswerID, CreationDate, Score, ViewCount, Body, OwnerUserId, LastEditorUserID, LastEditorDisplayName, LastEditDate, LastActivityDate, Title, Tags, AnswerCount, CommentCount, FavoriteCount |
| Tags     | Id, TagName, Count                                           |
| Users    | Id, Reputation, CreationDate, DisplayName, LastAccessDate, WebsiteUrl, Location, About Me, Views, Upvotes, Downvotes, AccountID |
| Votes    | Id, PostId, VoteTypeId (2->upvote), CreationDate             |

## Processing Steps

Task: Query multiple tags, get topK users that are related to these tags.

- [ ] Step1 (Posts.xml)

1. In Posts.xml, we only need entries with PostTypeId == 2.

2. Separate Post.tags into individual tags.

```user_tag```

| Post.Id | Post.OwnerUserId | Tag  |
| ------- | ---------------- | ---- |
|         |                  |      |

Primary key: composite key (Id, Tag)

- [ ] Step2 (user_tag, Votes.xml)

```sql
SELECT user_tag.Tag, user_tag_map_res.OwnerUserId, SUM (
                                                                CASE WHEN v.VoteTypeID=2
                                                                THEN 1
                                                                DEFAULT 0
                                                                END) AS upvotes
FROM Votes JOIN user_tag ON V.Id=P.ID
GROUP BY user_tag.Tag, user_tag.OwnerUserId
```

```user_upvote```

| Tag  | OwnerUserId | Upvotes |
| ---- | ----------- | ------- |
|      |             |         |

Primary Key: composite key (tag, OwnerUserId)

- [ ] Step3 (Badges.xml)

Generate a new table called WeightedBadges.

Assume gold == 5 scores, silver == 3 scores, golden == 1 scores.

| UserId | BadgeScore |
| ------ | ---------- |
|        |            |

```sql
SELECT UserID, SUM(CASW
                   WHEN Class=1, THEN 5,
                   WHEN Class=2, THEN 3,
                   WHEN Class=3, THEN 1,
                   DEFAULT 0
                   END) AS BadgeScore
From Badges
GROUPBY UserId
```

- [ ] Step4 (WeightedBadges, Users.xml, user_upvotes)

| Tag  | UserId | Upvote | BadgeScore | Reputation | LastActiveDate |
| ---- | ------ | ------ | ---------- | ---------- | -------------- |
|      |        |        |            |            | (filter)       |

----->

We have WeightedBadge, User, use_upvote, we want to generate the allScore table.

| Tag  | OwnerUserId | AllScore |
| ---- | ----------- | -------- |
|      |             |          |

```sql
SELECT tbl2.UserId, (tbl2.BadgeScore + tbl2.Reputation + tbl2.upvtes) AS AllScore
FROM (weightBadge w JOIN Users u ON w, UserId = u.Id) AS tbl1 JOIN BBB b ON tbl1.UserId = b.OwnerUserId AS tbl2
```



```allScore```

| Tag  | OwnerUserId | AllScore |
| ---- | ----------- | -------- |
|      |             |          |

- [ ] Step5 (allScore, query keywords)

```sql
SELECT OwnerUserId, allScore
FROM allScore
WHERE tag = "xxx"
ORDER BY allScore
LIMIT K
```

