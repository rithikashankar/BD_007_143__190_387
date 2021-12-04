import sys
import re
import json
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pickle
# sklearn
from sklearn.svm import LinearSVC

from sklearn.linear_model import LogisticRegression

from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import confusion_matrix, classification_report
  
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
from pyspark.ml.feature import StringIndexer,StopWordsRemover
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator




def preprocess(data):
	dat=data.collect()
	if len(dat)>0:
		dat=dat[0]
		col=['label','text']
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
		tokenizer = Tokenizer(inputCol='text', outputCol='words_token')
		remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
		
		
		pipeline = Pipeline(stages=[tokenizer, remover])
		pipelineFit = pipeline.fit(df)
		train_df = pipelineFit.transform(df)
		dnp=[(int(row['label']),row['text']) for row in train_df.collect()]    
		#X_train=np.array(train_df.select('words_clean').collect(),dtype=object)
		dnp=np.array(dnp)
		X_train=dnp[:,1]
		y_train=dnp[:,0]
		#y_train=np.array(train_df.select('label').collect())
		
		X_train=vectoriser.fit_transform(X_train)
		print(X_train)

		#print(np.array(train_df.select('features','label').collect(),dtype=object))
		 
		c=np.unique(y_train)

		#bnb.partial_fit(X_train,y_train,classes=c)
		#mnb.partial_fit(X_train,y_train,classes=c)
		#sgd.partial_fit(X_train,y_train,classes=c)
		#print(mnb.get_params())


     # df = dat.toDF()
     # df.printSchema()
     # df.show(truncate=False)
if __name__ == "__main__":
	bnb=BernoulliNB()
	mnb=MultinomialNB()
	sgd=SGDClassifier()
	vectoriser = TfidfVectorizer(ngram_range=(1,1), max_features=500000)
	sc   = SparkContext(appName='test')
	spark = SparkSession.builder.appName('sparkdf').getOrCreate()
	ssc  = StreamingContext(sc, 3)
	sqlContext = SQLContext(sc)
	lines = ssc.socketTextStream("localhost", 6100)
	words=lines.flatMap(lambda line: line.split('\n'))
	lines.foreachRDD(preprocess)
	print("looped")
    

	ssc.start()             # Start the computation
	print("start")
	ssc.awaitTermination()  # Wait for the computation to terminate
	print("Terminated")
	ssc.stop()
	pickle.dump(bnb,open('bnb.sav','wb'))
	pickle.dump(sgd,open('sgd.sav','wb'))
	pickle.dump(mnb,open('mnb.sav','wb'))
 #   main()


