from pyspark.sql import * 
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import re
import sys, operator


inputs = sys.argv[1]
predict_tag = sys.argv[2]
output = sys.argv[2]
 
conf = SparkConf().setAppName('auto-tag')
sc = SparkContext()
sqlContext = SQLContext(sc)

def encode(tags):
    if(tags.find(predict_tag) != -1):
        return 1.0
    else:
        return 0.0

posts_df = sqlContext.read.parquet(inputs + '/posts_table')
tags_df = sqlContext.read.parquet(inputs + '/tags_table')

posts_df.registerTempTable('posts')

posts_questions = sqlContext.sql("""
    SELECT id, tags, title, body
    FROM posts    
    where title IS NOT NULL    
""")

tags_sql = tags_df.select('id', 'tagname', 'counttag')

posts_encoded = posts_questions.map(lambda (id, tags, title, body) : Row(Id = id, Label = encode(tags), Text = (title+""+body)))

post_set = sqlContext.createDataFrame(posts_encoded)

positive = post_set.filter(post_set.Label > 0.0).cache()
negative = post_set.filter(post_set.Label < 1.0).cache()
 
# Sample without replacement (False)
positiveTrain = positive.sample(False, 0.9).cache()
negativeTrain = negative.sample(False, 0.9).cache()
training = positiveTrain.unionAll(negativeTrain)
 
negTrainTmp1 = negativeTrain.withColumnRenamed("Label", "Flag")
negativeTrainTmp = negTrainTmp1.select(negTrainTmp1.Id, negTrainTmp1.Flag)
 
 
negativeTest = negative.join( negativeTrainTmp, negative.Id == negativeTrainTmp.Id, "LeftOuter").\
                        filter("Flag is null").\
                        select(negative.Id, negative.Text, negative.Label)
 
posTrainTmp1 = positiveTrain.withColumnRenamed("Label", "Flag")
positiveTrainTmp = posTrainTmp1.select(posTrainTmp1.Id, posTrainTmp1.Flag)
 
positiveTest = positive.join( positiveTrainTmp, positive.Id == positiveTrainTmp.Id, "LeftOuter").\
                        filter("Flag is null").\
                        select(positive.Id, positive.Text, positive.Label)
testing = negativeTest.unionAll(positiveTest)
 
 
# CREATE MODEL
numFeatures = 64000
numEpochs = 30
regParam = 0.02
 
tokenizer = Tokenizer().setInputCol("Text").setOutputCol("Words")
#tokenized = tokenizer.transform(training)
hashingTF = HashingTF().setNumFeatures(numFeatures).\
                setInputCol("Words").setOutputCol("Features")
lr = LogisticRegression().setMaxIter(numEpochs).setRegParam(regParam).\
                                    setFeaturesCol("Features").setLabelCol("Label").\
                                    setRawPredictionCol("Score").setPredictionCol("Prediction")
pipeline = Pipeline().setStages([tokenizer, hashingTF, lr])
 

model = pipeline.fit(training) 
 
testingResult = model.transform(testing)
testingResultScores = testingResult.select("Prediction", "Label").rdd.map( lambda r: (float(r[0]), float(r[1])))
bc = BinaryClassificationMetrics(testingResultScores)
print bc

newbc = bc.call('roc')
print newbc.take(20)
#roc = bc.areaUnderROC()
#print("Area under the ROC:",  roc)