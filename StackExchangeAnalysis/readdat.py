from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys, operator
from pyspark.sql import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
#from lxml import etree
import xml.dom.minidom as minidom
from datetime import datetime

inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('test')
sc = SparkContext()
sqlContext = SQLContext(sc)

commentFile = sc.textFile(inputs + '/Comments.xml.gz')
postFile = sc.textFile(inputs + '/Posts.xml.gz')
userFile = sc.textFile(inputs + '/Users.xml.gz')
tagFile = sc.textFile(inputs + '/Tags.xml.gz')


def readrow(string):
    el = etree.fromstring(string)
    toReturn = (el.xpath('//@PostId'),el.xpath('//@Text'),el.xpath('//@CreationDate') \
                ,el.xpath('//@UserId'))
    return toReturn

def users_xml(string):
    tree = minidom.parseString(string)
    itemlist = tree.getElementsByTagName('row')
    Reputation = itemlist[0].attributes['Reputation'].value
    CreationDate = itemlist[0].attributes['CreationDate'].value
    DisplayName = itemlist[0].attributes['DisplayName'].value
    if(itemlist[0].hasAttribute("Location")):
        Location = itemlist[0].attributes['Location'].value
    else:
        Location = None   
    Views = itemlist[0].attributes['Views'].value
    UpVotes = itemlist[0].attributes['UpVotes'].value
    DownVotes = itemlist[0].attributes['DownVotes'].value
    AccountId = itemlist[0].attributes['AccountId'].value    
    if(itemlist[0].hasAttribute("Age")):
        Age = itemlist[0].attributes['Age'].value
    else:
        Age = None
        
    return (Reputation,CreationDate,DisplayName,Location,Views,UpVotes,DownVotes,AccountId,Age)
    

def comments_xml(string):
    tree = minidom.parseString(string)
    itemlist = tree.getElementsByTagName('row')
    postid = itemlist[0].attributes['PostId'].value
    score = itemlist[0].attributes['Score'].value
    text = itemlist[0].attributes['Text'].value
    creationdate = itemlist[0].attributes['CreationDate'].value    
    if(itemlist[0].hasAttribute("UserId")):
        userid = itemlist[0].attributes['UserId'].value
    else:
        userid = None
        
    return (postid,score,text,creationdate,userid)

def posts_xml(string):
    tree = minidom.parseString(string)
    itemlist = tree.getElementsByTagName('row')
    Id = itemlist[0].attributes['Id'].value
    PostTypeId = itemlist[0].attributes['PostTypeId'].value
    if(itemlist[0].hasAttribute("AcceptedAnswerId")):
        AcceptedAnswerId = itemlist[0].attributes['AcceptedAnswerId'].value
    else:
        AcceptedAnswerId = None
    CreationDate = itemlist[0].attributes['CreationDate'].value    
    #CreationDate = CreationDatet.strftime('%s')    
    Score = itemlist[0].attributes['Score'].value
    Body = itemlist[0].attributes['Body'].value
    if(itemlist[0].hasAttribute("ViewCount")):
        ViewCount = itemlist[0].attributes['ViewCount'].value
    else:
        ViewCount = None    
    if(itemlist[0].hasAttribute("Title")):
        Title = itemlist[0].attributes['Title'].value
    else:
        Title = None
     
    if(itemlist[0].hasAttribute("Tags")):
        Tags = itemlist[0].attributes['Tags'].value
    else:
        Tags = None 
                
    if(itemlist[0].hasAttribute("AnswerCount")):
        AnswerCount = itemlist[0].attributes['AnswerCount'].value
    else:
        AnswerCount = None
            
    if(itemlist[0].hasAttribute("CommentCount")):
        CommentCount = itemlist[0].attributes['CommentCount'].value
    else:
        CommentCount = None    
    
    if(itemlist[0].hasAttribute("ClosedDate")):
        ClosedDate = itemlist[0].attributes['ClosedDate'].value        
    else:
        ClosedDate = None
                
    if(itemlist[0].hasAttribute("OwnerUserId")):
        OwnerUserId = itemlist[0].attributes['OwnerUserId'].value
    else:
        OwnerUserId = None    
        
           
    return (Id,PostTypeId,AcceptedAnswerId,CreationDate,Score,Body,ViewCount,Title,Tags,AnswerCount,CommentCount,ClosedDate,OwnerUserId)

def tags_xml(string):
    tree = minidom.parseString(string)
    itemlist = tree.getElementsByTagName('row')
    Id = itemlist[0].attributes['Id'].value
    TagName = itemlist[0].attributes['TagName'].value
    Count = itemlist[0].attributes['Count'].value
    return (Id, TagName, Count)
    


commentsXml = commentFile.map(lambda line: line.strip().encode('utf-8')) \
    .filter(lambda line: not line.startswith("<?xml version=")) \
    .filter(lambda line: line != "<comments>" and line != "</comments>")
    
postsXml = postFile.map(lambda line: line.strip().encode('utf-8')) \
    .filter(lambda line: not line.startswith("<?xml version=")) \
    .filter(lambda line: line != "<posts>" and line != "</posts>")

usersXml = userFile.map(lambda line: line.strip().encode('utf-8')) \
    .filter(lambda line: not line.startswith("<?xml version=")) \
    .filter(lambda line: line != "<users>" and line != "</users>")

tagsXml = tagFile.map(lambda line: line.strip().encode('utf-8')) \
    .filter(lambda line: not line.startswith("<?xml version=")) \
    .filter(lambda line: line != "<tags>" and line != "</tags>")

 
comments = commentsXml.map(comments_xml)
posts = postsXml.map(posts_xml)
users = usersXml.map(users_xml)
tags = tagsXml.map(tags_xml)

commentschema = StructType([
    StructField('postid', StringType(), False),
    StructField('score', StringType(), False),
    StructField('text', StringType(), False),
    StructField('creationdate', StringType(), False),
    StructField('userid', StringType(), False)
])

postschema = StructType([
    StructField('id', StringType(), False),
    StructField('posttypeid', StringType(), False),
    StructField('acceptedanswerid', StringType(), False),
    StructField('creationdate', StringType(), False),
    StructField('score', StringType(), False),
    StructField('body', StringType(), False),
    StructField('viewcount', StringType(), False),
    StructField('title', StringType(), False),
    StructField('tags', StringType(), False),
    StructField('answercount', StringType(), False),
    StructField('commentcount', StringType(), False),
    StructField('closeddate', StringType(), False),
    StructField('owneruserid', StringType(), False)
])

userschema = StructType([
    StructField('reputation', StringType(), False),
    StructField('creationDate', StringType(), False),
    StructField('displayname', StringType(), False),
    StructField('location', StringType(), False),
    StructField('views', StringType(), False),
    StructField('upvotes', StringType(), False),
    StructField('downvotes', StringType(), False),
    StructField('accountid', StringType(), False),
    StructField('age', StringType(), False) 
])

tagschema = StructType([
    StructField('id', StringType(), False),
    StructField('tagname', StringType(), False),
    StructField('counttag', StringType(), False)    
])

comments_table = sqlContext.createDataFrame(comments, commentschema)
posts_table = sqlContext.createDataFrame(posts, postschema)
users_table = sqlContext.createDataFrame(users, userschema)
tags_table = sqlContext.createDataFrame(tags, tagschema)

comments_table.write.format('parquet').save(output + '/comments_table')
posts_table.write.format('parquet').save(output + '/posts_table')
users_table.write.format('parquet').save(output + '/users_table')
tags_table.write.format('parquet').save(output + '/tags_table')

print tags_table.rdd.take(10)
#tags_table.show(20)
#print commentsLabeled.rdd.take(20)