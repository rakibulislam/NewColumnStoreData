#
# Generate the test data set
# generate N rows, K columns  
# each field is long 
# By Jin Chen

import json
import sys

if(len(sys.argv)<4):
    print "Usage:",sys.argv[0]," <numObjects> <numColumns> <filename> "
    sys.exit(1)
    
print sys.argv
value = 1 
numObjects = int(sys.argv[1])
numFields = int(sys.argv[2])
#filename = sys.argv[3]+sys.argv[1]+"Obj"+sys.argv[2]+"Col.txt"
filename = sys.argv[3]
f = open(filename,'w')
print "Generate file with ",numObjects, "rows",numFields, " columns", "output file ",filename 
for i in range(0,numObjects):
    # empty the data set 
    data = {};
    for j in range(1,numFields+1):
        data[j]=value
    f.write(json.dumps(data))
    f.write("\n")
f.close()


