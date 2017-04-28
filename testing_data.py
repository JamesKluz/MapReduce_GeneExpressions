#NOTES: AD-> 4, 5     MCI-> 2, 3     NCI -> 1

import sys
import math
from pyspark import SparkContext
# from pyspark.sql import SQLContext
# from pyspark.sql.types import * 
sc = SparkContext()

####################### LOAD AND FORMAT GENE CLUSTERS --> (Entrex_id, cluster) ##################################

def split_cluster(x):
  cluster_id = x[0]
  entrez_list = x[4].split(';')
  entrez_cluster_pair_list = []
  for e_id in entrez_list:
    entrez_cluster_pair_list.append((e_id, cluster_id))
  return entrez_cluster_pair_list

clusters = sc.textFile("gene_cluster.csv").map(lambda x: x.split(','))
# first_5 = clusters.take(5)
# print(first_5[1][0], " -> ", first_5[1][4].split(';'))
# print(first_5[2][0], " -> ", first_5[2][4].split(';'))
# print(first_5[3][0], " -> ", first_5[3][4].split(';'))
header = clusters.first()
clusters = clusters.filter(lambda x: x != header)
clusters = clusters.flatMap(lambda x: split_cluster(x))
print(clusters.take(10))

####################### LOAD AND FORMAT ROSMAP --> (Entrex_id, (patient_id, diagnosis, value)) ##################################

def split_rosmap(x):
  patient_id = x[0]
  diagnosis = x[1]
  entrez_patient_pair_list = []
  for i in range(len(x))[2:]:
    #NOT SURE IF IT'S OKAY TO ACCESS THE HEADER IN THIS FUNCTION BUT I DON'T SEE ANOTHER SOLUTION
    entrez_patient_pair_list.append((header_rosmap[i], [patient_id, diagnosis, x[i]]))
  return entrez_patient_pair_list

rosmap = sc.textFile("ROSMAP_RNASeq_entrez.csv").map(lambda x: x.split(','))
header_rosmap = rosmap.first()
# print(header_rosmap[0], header_rosmap[1], header_rosmap[2], header_rosmap[3], header_rosmap[4], header_rosmap[5], header_rosmap[6])
#NOT SURE IF IT'S OKAY TO FILTER OUT MCI AT THS STEP (ASK PROFESSOR)
rosmap = rosmap.filter(lambda x: x != header_rosmap and x[1] != 'NA' and x[1] != '2' and x[1] != '3' and x[1] != '6')
# rosmap = rosmap.filter(lambda x: x[1] != 'NA')
rosmap = rosmap.flatMap(lambda x: split_rosmap(x))
first_5 = rosmap.take(5)
print(rosmap.take(5))
# print(rosmap.count())

####################### Join rdd's and get to 1st step ##################################

def isFloat(value):
  try:
    float(value)
    return True
  except:
    return False

def map_joined_cluster_rosmap(x):
  entrez_id = x[0]
  cluster_id = x[1][0]
  patient_id = x[1][1][0]
  diagnosis = x[1][1][1]
  value = x[1][1][2]
  if isFloat(value): 
    value = float(value)
  else:
    value = 0
  return ((patient_id, cluster_id), [diagnosis, value])

def reduce_joined_cluster_rosmap(x, y):
  return (x[0], x[1] + y[1])

step_1 = clusters.join(rosmap).map(lambda x: map_joined_cluster_rosmap(x)).reduceByKey(lambda x, y: reduce_joined_cluster_rosmap(x, y))
print(step_1.take(5))

####################### Find t scores ##################################
# def map_for_t_scores(x):
#   cluster_id = x[0][1]
#   diagnosis = x[1][0]
#   value = x[1][1]
#   value_squared = value*value
#   if diagnosis == '1':
#     diagnosis = 'NCI'
#   else:
#     diagnosis = 'AD'
#   return ((cluster_id, diagnosis), (1, value, value_squared))

# step_2 = step_1.map(lambda x: map_for_t_scores(x)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
# print(step_2.take(5)) 
def map_1_for_t_scores(x):
  cluster_id = x[0][1]
  diagnosis = x[1][0]
  value = x[1][1]
  value_squared = value*value
  if diagnosis == '1':
    #diagnosis = 'NCI'
    return (cluster_id, (1, value, value_squared, 0, 0, 0))
  else:
    #diagnosis = 'AD'
    return (cluster_id, (0, 0, 0, 1, value, value_squared))

step_2 = step_1.map(lambda x: map_1_for_t_scores(x)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4], x[5] + y[5])) 
print(step_2.take(5)) 

def map_2_for_t_scores(x):
  x_ad = x[1][4]/x[1][3]
  x_nci = x[1][1]/x[1][0]
  numerator = x_ad - x_nci
  std_ad_squared = x[1][5]/x[1][3] - (x_ad * x_ad)
  std_nci_squared = x[1][2]/x[1][0] - (x_nci * x_nci)  
  denominator = math.sqrt(std_ad_squared / x[1][3] + std_nci_squared / x[1][0])
  t = numerator/denominator
  return (x[0], (t, x_ad, x_nci, x[1][3], x[1][0], math.sqrt(std_ad_squared), math.sqrt(std_nci_squared)))  
 
step_2 = step_2.map(lambda x: map_2_for_t_scores(x))
print(step_2.take(5))

# step_2 = step_2.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
step_2 = step_2.sortBy(lambda x: -x[1][0])

####################### Get top k  t-scores ##################################

k = input("Enter a k: ")
k = int(k)

step_3 = step_2.take(k)

for i in step_3:
  print(i)




# fields = [StructField(field_name, StringType(), True) for field_name in header]
# print(len(fields))
# print(type(header))
#print(rosmap.take(2)[1])

# callData = sc.parallelize([["User1", "User2", 2], ["User1", "User3", 4], ["User2", "User1", 8]])

# calls = callData.flatMap(lambda record: [(record[0], record[2]), (record[1], record[2])])
# print calls.collect()
# # prints [('User1', 2), ('User2', 2), ('User1', 4), ('User3', 4), ('User2', 8), ('User1', 8)]

# reduce = calls.reduceByKey(lambda a, b: a + b)
# print reduce.collect()

