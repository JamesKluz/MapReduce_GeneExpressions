rosmap = []
with open('ROSMAP_RNASeq_entrez.csv', 'r') as input_file:
  for line in input_file:
    line_list = line.split(',')
    line_list[-1] = line_list[-1].strip()
    rosmap.append(line_list)
rosmap_transpose = []
for col in rosmap[0]:
  rosmap_transpose.append([line[0]])
for i in range(len(rosmap))[1:]:
  for j in range(len(rosmap[]))[1:]:
    rosmap_transpose[1]

print(rosmap[0])
print(rosmap[1])
