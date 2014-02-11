import subprocess
import os
import sys

numColumns = [ 2,4,8,16,24,32,40,48 ]
#numObjects = [ 100000,200000,500000,1000000,5000000,10000000 ]
numObjects = [ 10000000 ]
numRuns = 2

classpath="../.:../javax.json-1.0.3.jar"

#java -cp ../.:../javax.json-1.0.3.jar Argo1Test

prog=sys.argv[1]
#test Argo1 and Column Sotre 
# test the performance in different size 
for i in range(0,numRuns):
    for r in numObjects:
        for c in numColumns:
            testColumn=str(c); #last column
            filename = "tests/"+"test-"+str(c)+"C-"+str(r/1000)+"K"+".json"
            #print filename,c 
            #subprocess.Popen(["java","-cp",classpath,prog,filename,testColumn]); 
            results=subprocess.check_output(["java","-cp",classpath,prog,filename,testColumn]); 
            print str(r)+" "+str(c)+" "+results
            sys.exit(0)
             
    
