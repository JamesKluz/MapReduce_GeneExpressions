#Authors: James Kluz and Gil Dekel
#Big Data, Project 2, Part 1
#Given the ROSMAP and gene_cluster files, returns top k t-scores (AD, NCI)
#for the clusters given in gene_cluster file
#NOTES: AD-> 4, 5     MCI-> 2, 3     NCI -> 1    ???-> 6
#       ROSMAP has 3 diagnosis of 'NA'

import sys
import math   #for sqrt
from pyspark import SparkContext
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
header = clusters.first()
clusters = clusters.filter(lambda x: x != header)
clusters = clusters.flatMap(lambda x: split_cluster(x))

####################### LOAD AND FORMAT ROSMAP --> (Entrex_id, (patient_id, diagnosis, value)) ##################################

def split_rosmap(x):
  patient_id = x[0]
  diagnosis = x[1]
  entrez_patient_pair_list = []
  for i in range(len(x))[2:]:
    # NOT SURE IF IT'S OKAY TO ACCESS THE HEADER IN THIS FUNCTION BUT I DON'T SEE ANOTHER REASONABLE SOLUTION
    entrez_patient_pair_list.append((header_rosmap[i], [patient_id, diagnosis, x[i]]))
  return entrez_patient_pair_list

rosmap = sc.textFile("ROSMAP_RNASeq_entrez.csv").map(lambda x: x.split(','))
header_rosmap = rosmap.first()
# NOT SURE IF IT'S OKAY TO FILTER OUT NON-NCI / NON-AD AT THS STEP (ASK PROFESSOR)
# Filter out all patients where diagnosis is in ('NA', 2, 3, 6)
# this will make the join less brutal
rosmap = rosmap.filter(lambda x: x != header_rosmap and x[1] != 'NA' and x[1] != '2' and x[1] != '3' and x[1] != '6')
rosmap = rosmap.flatMap(lambda x: split_rosmap(x))
first_5 = rosmap.take(5)

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

####################### Find t scores ################################## 
# The 6-tuple value in the return pair represents: 
# (number of NCI, sum of NCI, sum of squared NCI, number of AD, sum of AD, sum of AD squared)
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

def map_2_for_t_scores(x):
  sum_of_ad = x[1][4]
  sum_of_nci = x[1][1]
  n_ad = x[1][3]
  n_nci = x[1][0]
  mean_ad = sum_of_ad/n_ad
  mean_nci = sum_of_nci/n_nci
  sum_of_ad_squared = x[1][5]
  sum_of_nci_squared = x[1][2]
  std_ad_squared = sum_of_ad_squared/n_ad - (mean_ad * mean_ad)
  std_nci_squared = sum_of_nci_squared/n_nci - (mean_nci * mean_nci)  
  denominator = math.sqrt(std_ad_squared / n_ad + std_nci_squared / n_nci)
  numerator = mean_ad - mean_nci
  t = numerator/denominator
  return (x[0], t)  

step_2 = step_2.map(lambda x: map_2_for_t_scores(x))
# Sort by t-score Descending
step_2 = step_2.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))

####################### Get top k  t-scores ##################################
while(True):
  k = input("Enter a k: ")
  k = int(k)
  step_3 = step_2.take(k)
  for i in step_3:
    print(i)