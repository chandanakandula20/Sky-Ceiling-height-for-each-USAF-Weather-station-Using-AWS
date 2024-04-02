from pyspark import SparkContext
import sys

def main():
	sc=SparkContext(appName='AvgDistancebyid')
	
	inputrdd=sc.textFile('/user/hadoop/inputC/Project_Data/*.gz')

	filteredrdd=inputrdd.filter(lambda line: line [78:84] != '999999' and int(line[84:85]) in [0,1,4,5,9])
	
	res_rdd = filteredrdd.map(lambda line : (line [4:10] , int(line[78:84])))

	res1_rdd=res_rdd.groupByKey().map(lambda z : (z[0], sum(z[1])/len(z[1])))

	res1_rdd.saveAsTextFile('/user/hadoop/projectoutput')

if __name__ == '__main__':
	main()