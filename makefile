#
# Makefile for cheetah programs
#

#JFLAGS = -g
CLASSPATH = .:./javax.json-1.0.3.jar
JFLAGS = -Xlint -cp ${CLASSPATH}
JC = javac


SimpleQueryExecutor:SimpleQueryExecutor.java
	${JC} ${JFLAGS} SimpleQueryExecutor.java
NewArgo1StoreEng:NewArgo1StoreEng.java
	${JC} ${JFLAGS} NewArgo1StoreEng.java
NewRowStoreEng:NewRowStoreEng.java
	${JC} ${JFLAGS} NewRowStoreEng.java
NewColStoreEng:NewColStoreEng.java
	${JC} ${JFLAGS} NewColStoreEng.java
NewRowColStoreEng:NewRowColStoreEng.java
	${JC} ${JFLAGS} NewRowColStoreEng.java
StoreEngine:StoreEngine.java
	${JC} ${JFLAGS} StoreEngine.java

clean:
	rm *.class
