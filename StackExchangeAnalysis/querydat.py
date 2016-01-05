from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys, operator
from pyspark.sql import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import DataFrameNaFunctions
from datetime import datetime, timedelta

# spark-submit --master yarn-client --packages com.databricks:spark-csv_2.11:1.2.0 querydat.py /user/amitt/output1 /user/amitt/output2

inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('test')
sc = SparkContext()
sqlContext = SQLContext(sc)

posts_df = sqlContext.read.parquet(inputs + '/posts_table')
comments_df = sqlContext.read.parquet(inputs + '/comments_table')
users_df = sqlContext.read.parquet(inputs + '/users_table')
tags_df = sqlContext.read.parquet(inputs + '/tags_table')


posts_df.registerTempTable('posts')
comments_df.registerTempTable('comments')
users_df.registerTempTable('users')
tags_df.registerTempTable('tags')

def calc_time(owneruserid,tag, creationdate, closeddate):
    creationtime = datetime.strptime(creationdate, '%Y-%m-%dT%H:%M:%S.%f')
    closedtime = datetime.strptime(closeddate, '%Y-%m-%dT%H:%M:%S.%f')
    diff = closedtime-creationtime
    difference = (diff.days * 86400 + diff.seconds)/3600.0
    #timedifference = abs(closedtime - creationtime).total_seconds() / 3600.0
    return (Row(userid = owneruserid,tags=tag , timediff = difference))


posts_questions = sqlContext.sql("""
    SELECT *
    FROM posts    
    where posttypeid = '1'
    
""")

questionposts = posts_questions.registerTempTable('questionposts')
#
posts_Top_answers = sqlContext.sql("""
    SELECT tags,answercount,commentcount
    FROM posts    
    where posttypeid = '2' and title IS NOT NULL
    Order by commentcount DESC
""")

#populartags since one year
popular_tags = sqlContext.sql("""
    SELECT tagname, counttag
    FROM tags where counttag > 10
    ORDER BY counttag DESC,  tagname ASC       
""")
#popular_tags.coalesce(1).write.format('com.databricks.spark.csv').save(output+'/populartags.csv')
#reputed-user
users_sql = sqlContext.sql("""
    SELECT displayname, location, (upvotes-downvotes) as positivevotes
    FROM users    
    ORDER BY positivevotes DESC     
""")
#users_sql.coalesce(1).write.format('com.databricks.spark.csv').save(output+'/reputedusers.csv')
#annonymous-users
annonymous_users = sqlContext.sql("""
    SELECT count(displayname) as annonymoususers
    FROM users             
""")
#annonymous_users.coalesce(1).write.format('com.databricks.spark.csv').save(output+'/annonymoususers.csv')


users_loc = users_df.dropna(how ='any', subset='location')
users_loc.registerTempTable('usersloc')
#users from countries
users_locsql = sqlContext.sql("""
    SELECT count(displayname) as users, location
    FROM usersloc
    GROUP BY location
    ORDER BY users DESC         
""")
#users_locsql.coalesce(1).write.format('com.databricks.spark.csv').save(output+'/userslocations.csv')
#questions with most answers
posts_Top_answers1 = sqlContext.sql("""
    SELECT tags,sum(answercount) as comments
    FROM posts    
    where posttypeid = '1'
    GROUP BY tags
    ORDER BY comments DESC
""")
#posts_Top_answers1.coalesce(1).write.format('com.databricks.spark.csv').save(output+'/topanswers.csv')
#questions with max time for closing(in hours)
testdates = sqlContext.sql("""
    SELECT owneruserid, tags,creationdate, closeddate
    FROM posts
    where posttypeid = '1' and owneruserid IS NOT NULL and closeddate IS NOT NULL   
""")

test1 =  testdates.map(lambda line: (line[0],line[1],line[2],line[3]))
test2 = test1.map(lambda (owneruserid,tags,creationdate,closeddate): calc_time(owneruserid,tags,creationdate,closeddate))
timetaken = sqlContext.createDataFrame(test2)
timetaken.registerTempTable('time')
longesttime = sqlContext.sql("""
    SELECT *
    FROM time
    ORDER BY timediff DESC
""")
#longesttime.coalesce(1).write.format('com.databricks.spark.csv').save(output+'/time.csv')

longesttime1 = sqlContext.sql("""
    SELECT count(tags) as notags
    FROM posts
    where title IS NULL    
""")

longesttime2 = sqlContext.sql("""
    SELECT count(tags) as withtags, tags
    FROM posts
    where title IS NOT NULL
    GROUP BY tags
    ORDER BY withtags DESC    
""")

longesttime3 = sqlContext.sql("""
    SELECT count(tags) as withtags
    FROM posts
    where title IS NOT NULL    
""")
longesttime4 = sqlContext.sql("""
    SELECT tags
    FROM posts
    where title IS NOT NULL    
""")

longesttime4.show(20)
longesttime2.show(20)