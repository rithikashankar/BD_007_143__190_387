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
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.metrics import confusion_matrix, classification_report
  
# creating sparksession and giving an app name
from sklearn.linear_model import SGDClassifier
from sklearn.naive_bayes import BernoulliNB
from sklearn.naive_bayes import MultinomialNB
from sklearn.cluster import MiniBatchKMeans  
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
		X_train=dnp[:,1]
		y_train=dnp[:,0]
		
		X_train=vectorizer.fit_transform(X_train)
		end=time.time()
		print(end-start,end=',')
		c=np.unique(y_train)
		start=time.time()
		mnb.partial_fit(X_train,y_train,classes=c)
		end=time.time()
		print(end-start,end=',')
		start=time.time()
		sgd.partial_fit(X_train,y_train,classes=c)
		end=time.time()
		print(end-start,end=',')
		start=time.time()
		bnb.partial_fit(X_train,y_train,classes=c)
		end=time.time()
		print(end-start)
		pickle.dump(mnb,open('mnb.sav','wb'))
		pickle.dump(bnb,open('bnb.sav','wb'))
		pickle.dump(sgd,open('sgd.sav','wb'))
		pickle.dump(vectorizer,open('vector.pk','wb'))
			
		# t0 = time.time()
		km.fit_transform(X_train)
		print("done in %0.3fs" % (time.time() - t0))
		print('\n')
	
		pickle.dump(km, open('km.sav', 'wb'))





if __name__ == "__main__":
	bnb=BernoulliNB()
	mnb=MultinomialNB()
	sgd=SGDClassifier(penalty='elasticnet')
	km = MiniBatchKMeans(n_clusters=2, init='k-means++', n_init=1,
                         batch_size=10000, verbose=0, compute_labels=True)
	
	vectorizer = HashingVectorizer(
    decode_error="ignore", n_features=100000, alternate_sign=False)
	sc   = SparkContext(appName='BD-project')
	spark = SparkSession.builder.appName('BD.ML').getOrCreate()
	ssc  = StreamingContext(sc, 5)
	sqlContext = SQLContext(sc)
	lines = ssc.socketTextStream("localhost", 6100)
	words=lines.flatMap(lambda line: line.split('\n'))
	lines.foreachRDD(preprocess)
	print('vector,mnb,sgd,bnb')
	ssc.start()             # Start the computation
	
	ssc.awaitTermination()  # Wait for the computation to terminate
	
	ssc.stop()
