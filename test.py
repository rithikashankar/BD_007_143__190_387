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
from sklearn.metrics import confusion_matrix, classification_report,accuracy_score
  
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
		mnrep=classification_report(y_test,ymnb,target_names=c,output_dict=True)
		mnacc=accuracy_score(y_test,ymnb)
		sgrep=classification_report(y_test,ysgd,target_names=c,output_dict=True)
		sgacc=accuracy_score(y_test,ysgd)
		bnrep=classification_report(y_test,ybnb,target_names=c,output_dict=True)
		bnacc=accuracy_score(y_test,ybnb)
		print('%s,%s,%f,%f,%f,%f'%('mnb','0',mnrep['0']['precision'],mnrep['0']['recall'],mnrep['0']['f1-score'],mnacc))
		print('%s,%s,%f,%f,%f,%f'%('mnb','4',mnrep['4']['precision'],mnrep['4']['recall'],mnrep['4']['f1-score'],mnacc))
		print('%s,%s,%f,%f,%f,%f'%('sgd','0',sgrep['0']['precision'],sgrep['0']['recall'],sgrep['0']['f1-score'],sgacc))
		print('%s,%s,%f,%f,%f,%f'%('sgd','4',sgrep['4']['precision'],sgrep['4']['recall'],sgrep['4']['f1-score'],sgacc))
		print('%s,%s,%f,%f,%f,%f'%('bnb','0',bnrep['0']['precision'],bnrep['0']['recall'],bnrep['0']['f1-score'],bnacc))
		print('%s,%s,%f,%f,%f,%f'%('bnb','4',bnrep['4']['precision'],bnrep['4']['recall'],bnrep['4']['f1-score'],bnacc))
		y_test2= np.where(y_test == 4, 1, 0)
		print(y_test2)
		ykm= km.predict(X_test)
		
		#print('p=', ykm)
		#print('fr=', y_test)
		print('ok')





if __name__ == "__main__":
	bnb=pickle.load(open('bnb.sav','rb'))
	mnb=pickle.load(open('mnb.sav','rb'))
	sgd=pickle.load(open('sgd.sav','rb'))
	vectorizer = pickle.load(open('vector.pk','rb'))
	km=pickle.load(open('km.sav','rb'))

	print('classifier,class,precision,recall,f1-score,accuracy')
	sc   = SparkContext(appName='test')
	spark = SparkSession.builder.appName('sparkdf').getOrCreate()
	ssc  = StreamingContext(sc, 2)
	sqlContext = SQLContext(sc)
	lines = ssc.socketTextStream("localhost", 6100)
	words=lines.flatMap(lambda line: line.split('\n'))
	lines.foreachRDD(preprocess)
	
	

	ssc.start()             # Start the computation
	
	ssc.awaitTermination()  # Wait for the computation to terminate
	
	ssc.stop()
