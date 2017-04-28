# Author James Kluz
# Takes in ROSMAP and gene_cluster.csv
# Input a cluster id and returns: (t-score, mean_ad, mean_nci, n_ad, n_nci, std_ad, std_nci)

import pandas as pd
import math

clusters = pd.read_csv('gene_cluster.csv')
rosmap = pd.read_csv('ROSMAP_RNASeq_entrez.csv')

while True :
  cluster_querry = input("Enter a cluster ID: ")
  #get row from clusters data frame correspongin to that ID
  querry = clusters.loc[clusters['ClusterID'] == int(cluster_querry)]
  if not querry.empty:
    #Get list of entrez id's associated to this cluster
    rosmap['sum'] = 0
    entrez_list = querry.get_value(querry.index[0], 'subunits(Entrez IDs)').split(';')
    for en_ID in entrez_list:
      rosmap['sum'] = rosmap['sum'] + rosmap[en_ID]
    #Patients with AD diagnosis
    rosmap['sum_squared'] = rosmap['sum'] * rosmap['sum']
    rosmapAD = pd.DataFrame(rosmap.loc[rosmap['DIAGNOSIS'].isin([4, 5])])
    statistics = rosmapAD['sum'].describe()
    N_ad = statistics['count']
    mean_ad = statistics['mean']
    variance_ad = rosmapAD['sum_squared'].describe()['mean'] - (mean_ad*mean_ad)
    #Patients with NCI diagnosis
    rosmapNCI = pd.DataFrame(rosmap.loc[rosmap['DIAGNOSIS'] == 1])  
    statistics = rosmapNCI['sum'].describe()
    N_nci = statistics['count']
    mean_nci = statistics['mean']
    variance_nci = rosmapNCI['sum_squared'].describe()['mean'] - mean_nci*mean_nci
    numerator = mean_ad - mean_nci
    denominator = math.sqrt(variance_ad/N_ad + variance_nci/N_nci)
    t = numerator / denominator
    print("T-score: ", t, mean_ad, mean_nci, N_ad, N_nci, math.sqrt(variance_ad), math.sqrt(variance_nci))
  else:
    print("No such ID")


