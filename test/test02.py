# encoding=utf8
"""SimpleApp"""

import sys
import os
from pyspark import SparkContext, SparkConf

print os.getenv('HADOOP_HOME')
conf = SparkConf.set("io.compression.codecs", "org.apache.hadoop.io.compress.Lz4Codec")

logFile = "application_1478165914493_2491600.lz4"
if len(sys.argv) > 1:
    print "spark 运行模式:", sys.argv[1]
    sc = SparkContext(sys.argv[1], 'simple demo', conf=conf)
else:
    sc = SparkContext(appName='simple demo1', conf=conf)

sc.setLogLevel('ERROR')

# logData = sc.textFile("/user/zhangshoudi/" + logFile).cache()
logData = sc.textFile("" + logFile)
counts = logData.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

#counts.saveAsTextFile("/user/zhangshoudi/20161205")
counts.saveAsTextFile('/user/zhangshoudi/20161205/02', 'org.apache.hadoop.io.compress.Lz4Codec')
