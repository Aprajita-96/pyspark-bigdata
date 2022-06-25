from pyspark import SparkContext 
sc = SparkContext("local")
lines = sc.textFile("file:///home/hadoop/eBooks/*.txt")
words = lines.flatMap(lambda a: a.split())
sized=words.map(lambda a:len(a))
result=sized.map(lambda a:(a,1)).reduceByKey(lambda a,b:a+b).filter(lambda a:a[1]>=495000).sortBy(lambda a:-a[1])
print(result.collect())