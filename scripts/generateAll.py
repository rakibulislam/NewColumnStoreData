import subprocess
import os

numColumns = [ 2,4,8,16,24,32,40,48 ]
numObjects = [ 100000,200000,500000,1000000,5000000,10000000 ]
#Usage: generateData.py  <numObjects> <numColumns> <filename> 

for r in numObjects:
    for c in numColumns:
        filename = "test-"+str(c)+"C-"+str(r/1000)+"K"+".json" 
        print filename 
        subprocess.Popen(["python","generateData.py",str(r),str(c),filename])

