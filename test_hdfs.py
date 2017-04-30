from pyspark import SparkContext
sc = SparkContext()

clusters = sc.textFile("gene_cluster.csv").map(lambda x: x.split(','))
clusters.saveAsTextFile("hdfs:///file.txt")