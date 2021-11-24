import sys
import re
import json
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
  
# creating sparksession and giving an app name

  
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel, StreamingKMeans
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import operator
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator




def preprocess(data):
	print(type(data))
	dat=data.collect()
	if len(dat)>0:
		dat=dat[0]
		col=['Sentiment','text']
		print(type(dat))
		j=json.loads(dat)
		rows=[]
		for i in j:
			l=(j[i]['feature0'],j[i]['feature1'])
			rows.append(l)
		df=spark.createDataFrame(rows,col)
          # df.printSchema()

          #cleaning the data
		df = df.na.replace('', None)
		df = df.na.drop()
		df = df.withColumn('text', F.lower(F.col('text')))
		df = df.withColumn('text', F.regexp_replace('text', r'http\S+', ''))
		df = df.withColumn('text', F.regexp_replace('text', '@\w+', ''))
		df = df.withColumn('text', F.regexp_replace('text', '#', ''))
		df = df.withColumn('text', F.regexp_replace('text', 'RT', ''))
		df = df.withColumn('text', F.regexp_replace('text', ':', ''))
		df = df.withColumn('text', F.regexp_replace('text', '[^a-zA-Z #]', ''))
		# df.show()
		df = df.na.drop()


		tokenizer = Tokenizer(inputCol="text", outputCol="words")
		hashtf = HashingTF(numFeatures=2**16, inputCol="words", outputCol='tf')
		idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
		label_stringIdx = StringIndexer(inputCol = 'Sentiment', outputCol = 'label')
		pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx])
	  
		pipelineFit = pipeline.fit(train_set)
		train_df = pipelineFit.transform(train_set)
		val_df = pipelineFit.transform(val_set)
		train_df.show(5)


     # df = dat.toDF()
     # df.printSchema()
     # df.show(truncate=False)
if __name__ == "__main__":
     
	sc   = SparkContext(appName='test')
	spark = SparkSession.builder.appName('sparkdf').getOrCreate()
	ssc  = StreamingContext(sc, 5)
	sqlContext = SQLContext(sc)
	lines = ssc.socketTextStream("localhost", 6100)
	words=lines.flatMap(lambda line: line.split('\n'))
	lines.foreachRDD(preprocess)
    

	ssc.start()             # Start the computation
	ssc.awaitTermination()  # Wait for the computation to terminate
 #   main()


