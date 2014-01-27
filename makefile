#
# Makefile for cheetah programs
#

#JFLAGS = -g
CLASSPATH = .:./javax.json-1.0.3.jar
JFLAGS = -Xlint -cp ${CLASSPATH}
JC = javac

HybridStore:HybridStore.java
	${JC} ${JFLAGS} HybridStore.java
ColStore:ColStore.java
	${JC} ${JFLAGS} ColStore.java
Argo3:Argo3.java
	${JC} ${JFLAGS} Argo3.java
Argo1:Argo1.java
	${JC} ${JFLAGS} Argo1.java
DataPopulator:DataPopulator.java
	${JC} ${JFLAGS} DataPopulator.java
RawCollectionsExample: RawCollectionsExample.java
	${JC} ${JFLAGS} RawCollectionsExample.java
DataBuffer: DataBuffer.java 
	${JC} ${JFLAGS} DataBuffer.java
TestBuffer: TestBuffer.java 
	${JC} ${JFLAGS} TestBuffer.java
SeqBufferEngine: SeqBufferEngine.java	User.java 
	${JC} ${JFLAGS} SeqBufferEngine.java User.java
TestTweet: TestTweet.java Tweet.java
	${JC} ${JFLAGS} TestTweet.java Tweet.java
ReadJsonUserGson: ReadJsonUseGson.java	User.java 
	${JC} ${JFLAGS} ReadJsonUseGson.java User.java
RingBufferEngine: RingBufferEngine.java	User.java 
	${JC} ${JFLAGS} RingBufferEngine.java User.java

clean:
	rm *.class
