import subprocess
import os

numColumns = [ 2,4,8 ]
numObjects = [10];
#numColumns = [ 2,4,8,16,24,32,40,48 ]
#numObjects = [ 100000,200000,500000,1000000,5000000,10000000 ]
#Usage: generateData.py  <numObjects> <numColumns> <filename> 

for r in numObjects:
    for c in numColumns:
        filename = "test-"+str(c)+"C-"+str(r/1000)+"K"+".json" 
        print filename 
        
	filename2 = "test-"+str(c)+"C-"+str(r/1000)+"K"+".def" 
	f = open(filename2,'w')
	print "Generating ", filename2
	fields = {};
	for i in range(1, c+1):
		f.write(str(i)+":LONG " )
	
	subprocess.Popen(["python","generateData.py",str(r),str(c),filename])

