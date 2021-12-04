import sys
import re
import json
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
  
# creating sparksession and giving an app name
from sklearn.linear_model import SGDClassifier
from sklearn.naive_bayes import BernoulliNB
from sklearn.naive_bayes import MultinomialNB
  
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
		col=['label','text']
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
		hashtf = HashingTF(numFeatures=2**18, inputCol="words", outputCol='tf')
		idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
		
		pipeline = Pipeline(stages=[tokenizer, hashtf, idf])
	  
		pipelineFit = pipeline.fit(df)
		train_df = pipelineFit.transform(df)
		print(np.array(train_df.select('features','tf','label').collect()))
		
		classes=np.unique(y_train)

		bnb.partial_fit(x_train,y_train)
		mnb.partial_fit(x_train,y_train)
		sgd.partial_fit(x_train,y_train)


     # df = dat.toDF()
     # df.printSchema()
     # df.show(truncate=False)
if __name__ == "__main__":
	bnb=BernoulliNB()
	mnb=MultinomialNB()
	sgd=SGDClassifier()
     
	sc   = SparkContext(appName='test')
	spark = SparkSession.builder.appName('sparkdf').getOrCreate()
	ssc  = StreamingContext(sc, 3)
	sqlContext = SQLContext(sc)
	lines = ssc.socketTextStream("localhost", 6100)
	words=lines.flatMap(lambda line: line.split('\n'))
	lines.foreachRDD(preprocess)
    

	ssc.start()             # Start the computation
	ssc.awaitTermination()  # Wait for the computation to terminate
	ssc.stop()
 #   main()


