#AUTHOR: James Kluz
#Input: (expects ROSMAP.csv and gene_clusters.csv to be in the data directory)
#Output: (for now) splits rosmap into 5 collections based on diagnosis and transposes

import os
data_dir = '/Users/jameskluz/Documents/Documents/Spring_2017/Big_Data/Projects/Project2/'
rosmap_file = 'ROSMAP_RNASeq_entrez.csv'
gene_cluster_file = 'gene_cluster.csv'

# open rosmap and split into 6 lists
# 1) AD    ('4' and '5')
# 2) MCI   ('2' and '3')
# 3) NCI   ('1')
# 4) Other ('NA' and '6')
# 5) Whole file             -> will need this when saved as HDFS
# 6) Whole file with entrez values saved as pairs -> (entrez_value, diagnosis)      -> just in case he doesn't like that I'm splitting diagnosis
rosmap_ad = []
rosmap_nci = []
rosmap_mci = []
rosmap_other = []
rosmap = []
rosmap_w_diagnosis = []
with open(data_dir + rosmap_file, 'r') as input_file:
  for line in input_file:
    line_list = line.split(',')
    line_list[-1] = line_list[-1].strip()
    rosmap.append(line_list)
    #This is the first line
    if line_list[1] == 'DIAGNOSIS':
      rosmap_ad.append(line_list)
      rosmap_mci.append(line_list)
      rosmap_nci.append(line_list)
      rosmap_other.append(line_list)
      rosmap_w_diagnosis.append(line_list)
    else: 
      if line_list[1] == '1':
        rosmap_nci.append(line_list)
      elif line_list[1] == '2' or line_list[1] == '3':
        rosmap_mci.append(line_list)
      elif line_list[1] == '4' or line_list == '5':
        rosmap_ad.append(line_list)
      else:
        rosmap_other.append(line_list) 
      #pair diagnosis with gene value  
      line_list_w_diagnosis = []
      for i in range(len(line_list)):
        if i < 2:
          line_list_w_diagnosis.append(line_list[i])
        else:  
          line_list_w_diagnosis.append((line_list[i], line_list[1]))
      rosmap_w_diagnosis.append(line_list_w_diagnosis)

# Transpose all 2D lists except for original
rosmap_ad_T = []
rosmap_nci_T = []
rosmap_mci_T = []
rosmap_other_T = []
rosmap_w_diagnosis_T = []

for i in list(zip(*rosmap_ad)):
  rosmap_ad_T.append(list(i))

for i in list(zip(*rosmap_nci)):
  rosmap_nci_T.append(list(i))

for i in list(zip(*rosmap_mci)):
  rosmap_mci_T.append(list(i))

for i in list(zip(*rosmap_other)):
  rosmap_other_T.append(list(i))

for i in list(zip(*rosmap_w_diagnosis)):
  rosmap_w_diagnosis_T.append(list(i))

# FOR TESTING ONLY  -> check first few rows and first 10 columns
# line_0 = rosmap_ad_T[0][:10]
# line_1 = rosmap_ad_T[1][:10]
# line_2 = rosmap_ad_T[2][:10]
# print(line_0)
# print(line_1)
# print(line_2)
# line_0 = rosmap_nci_T[0][:10]
# line_1 = rosmap_nci_T[1][:10]
# line_2 = rosmap_nci_T[2][:10]
# print(line_0)
# print(line_1)
# print(line_2)
# line_0 = rosmap_mci_T[0][:10]
# line_1 = rosmap_mci_T[1][:10]
# line_2 = rosmap_mci_T[2][:10]
# print(line_0)
# print(line_1)
# print(line_2)
# line_0 = rosmap_other_T[0][:10]
# line_1 = rosmap_other_T[1][:10]
# line_2 = rosmap_other_T[2][:10]
# print(line_0)
# print(line_1)
# print(line_2)
# line_0 = rosmap_w_diagnosis_T[0][:10]
# line_1 = rosmap_w_diagnosis_T[1][:10]
# line_2 = rosmap_w_diagnosis_T[2][:10]
# line_3 = rosmap_w_diagnosis_T[3][:10]
# line_4 = rosmap_w_diagnosis_T[4][:10]
# line_5 = rosmap_w_diagnosis_T[5][:10]
# print(line_0)
# print(line_1)
# print(line_2)
# print(line_3)
# print(line_4)
# print(line_5)

# LATER THIS IS WHERE WE WILL SAVE AS HFDS
# For now saving as .csv

with open(data_dir + "ROSMAP_transpose_ad.csv", 'w') as out_file:
  for row in rosmap_ad_T:
    for cell in row:
      out_file.write(cell + ",")
    out_file.write("\n")

with open(data_dir + "ROSMAP_transpose_nci.csv", 'w') as out_file:
  for row in rosmap_nci_T:
    for cell in row:
      out_file.write(cell + ",")
    out_file.write("\n")

with open(data_dir + "ROSMAP_transpose_mci.csv", 'w') as out_file:
  for row in rosmap_mci_T:
    for cell in row:
      out_file.write(cell + ",")
    out_file.write("\n")

with open(data_dir + "ROSMAP_transpose_other.csv", 'w') as out_file:
  for row in rosmap_other_T:
    for cell in row:
      out_file.write(cell + ",")
    out_file.write("\n")

with open(data_dir + "ROSMAP_transpose_w_diagnosis.csv", 'w') as out_file:
  for row in rosmap_w_diagnosis_T:
    for cell in row:
      if isinstance(cell, tuple):
        out_file.write("(" + cell[0] + ";" + cell[1] + "),")
      else:
        out_file.write(cell + ",")
    out_file.write("\n")



