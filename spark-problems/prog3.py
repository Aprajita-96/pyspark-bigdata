from pyspark import SparkContext 
sc = SparkContext("local")
lines = sc.textFile("nyc_taxi_data_2014.csv")
records = lines.map(lambda a:a.split(',')).filter(lambda a: a[0] != 'vendor_id')
nyc2=records.map(lambda a:(a[3],a[4],a[12])).filter(lambda a:eval(a[0])>=1 and eval(a[0])<=9)
nyc_data= nyc2.map(lambda a:(eval(a[0]),(1,eval(a[1]),eval(a[2])))).reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2])).map(lambda a: (a[0] , a[1][0] , a[1][1]/a[1][0] , a[1][2]/a[1][0])).sortBy(lambda a :a[0])
print(nyc_data.collect())