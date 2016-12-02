# encoding=utf8
"""SimpleApp"""

import sys
import os
from pyspark import SparkContext, SparkConf

print os.environ['SPARK_HOME'], "heihei"
print os.environ['PYTHONPATH'], "heihei"

logFile = "test.iml"
print "spark 运行模式:", sys.argv[1]
sc = SparkContext(sys.argv[1], 'simple demo')
sc.setLogLevel('ERROR')

logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
