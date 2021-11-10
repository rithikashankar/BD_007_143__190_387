import sys
import re

from pyspark import SparkContext
from pyspark.sql.context import SQLContext
import spark.implicits._
from pyspark.sql import Row
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import operator
# if __name__ == "__main__":
#     conf = SparkConf().setAppName("Listening to a tcp port")
#     sc   = SparkContext(conf=conf)
#     ssc  = StreamingContext(sc, 1)

#     jsonn = ssc.socketTextStream("localhost", 6100)
#     jsonn.pprint(1)
#     r=jsonn.flatMap(lambda x:x.split('\n'))
#     r1=r.map(lambda x:x.strip().split(',',1))


    

#     ssc.start()             # Start the computation
#     ssc.awaitTermination()  # Wait for the computation to terminate

sc = SparkContext(appName="test")
ssc = StreamingContext(sc, 1)
sqlContext = SQLContext(sc)

def streamrdd_to_df(srdd):
    sdf = sqlContext.createDataFrame(srdd)
    sdf.show(n=2, truncate=False)
    return sdf

def main():
    indata = ssc.socketTextStream('localhost',6100)
    inrdd = indata.map(lambda r: tuple(r))
    df=inrdd.toDF()
    df.show()

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
