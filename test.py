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
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import HashingVectorizer
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
import time



def preprocess(data):
	dat=data.collect()
	if len(dat)>0:
		start=time.time()
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
		X_test=dnp[:,1]
		y_test=dnp[:,0]
		
		X_test=vectorizer.fit_transform(X_test)

		c=np.unique(y_test)
		ymnb=mnb.predict(X_test)
		ysgd=sgd.predict(X_test)
		ybnb=bnb.predict(X_test)
		print('MNB\n',classification_report(y_test,ymnb,target_names=c))
		print('SGD\n',classification_report(y_test,ysgd,target_names=c))
		print('BNB\n',classification_report(y_test,ybnb,target_names=c))
		end=time.time()
		print(end-start)




if __name__ == "__main__":
	bnb=pickle.load(open('bnb.sav','rb'))
	mnb=pickle.load(open('mnb.sav','rb'))
	sgd=pickle.load(open('sgd.sav','rb'))
	vectorizer = pickle.load(open('vector.pk','rb'))
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