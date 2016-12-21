## Synopsis  
Implement Naive Bayes classification on Apache Hadoop.

##Dataset Details  
Data used is from Reuters Corpus which is a set of news stories split into a
hierarchy of categories. There are multiple class labels per document. This is a multi-class classification problem with following 4 classes
• CCAT: Corporate/Industrial
• ECAT: Economics
• GCAT: Government/Social
• MCAT: Markets

Datasets available at: https://ugammd.blob.core.windows.net/rcv1/

##Method  
There are 5 Map-Reduce java classes chained in a specific manner. First, the training data is cleaned, all data counts are calculated and passed along, train the NB classifier, clean test data and finally apply the algorithm.

```
bin/hadoop jar NBTrain.jar NBTrain input.txt NBTrainOutput		
bin/hadoop jar newNBTest.jar newNBTest NBTrainOutput/ newNBTestIntermediate newNBTestOutput
bin/hadoop jar NBTestFinal.jar NBTestFinal test.txt NBTestFinalOutput
bin/hadoop jar NBJoin.jar NBJoin newNBTestOutput/ NBTestFinalOutput/ NBJoinOutput
bin/hadoop jar NBClassify.jar NBClassify NBJoinOutput/ NBClassifyIntermediate NBClassifyOutput
```
