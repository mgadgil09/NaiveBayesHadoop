1. I have 5 Map Reduce files and I have tested only on RCV1.small_Train.txt and RCV1.small_test.txt. I did not get time to configure Microsoft Azure so I could not test on large data. 
Also, I have not calculated the maximum probability for each document. Hence my final output file has shuffled and sorted outputs. So the keys are sorted alphabetically. SO it is required to manually search for probabilities for all classes for a particular CLass.
Following is the sequence to run the Map reduce files:

bin/hadoop jar NBTrain.jar NBTrain input.txt NBTrainOutput		
bin/hadoop jar newNBTest.jar newNBTest NBTrainOutput/ newNBTestIntermediate newNBTestOutput
bin/hadoop jar NBTestFinal.jar NBTestFinal test.txt NBTestFinalOutput
bin/hadoop jar NBJoin.jar NBJoin newNBTestOutput/ NBTestFinalOutput/ NBJoinOutput
bin/hadoop jar NBClassify.jar NBClassify NBJoinOutput/ NBClassifyIntermediate NBClassifyOutput


Answers:

Q1) For the RCV1.small data test I have taken the readings. For reducers 2,4,6,8,10:

	 n	 Total time from Train to Probability calculation
      ---------------------------------------------------------------
      
         2   |			33s	
         
         4   |                  36s
         
         6   |                  40s
         
         8   |                  43s
         
         10  |                  41s
         
         if we plot a graph for this we can deduce that as the number of reduce tasks are increased, the running time also increases.
         the graph wil be exponential almost linear.
         As per my observations, as we increase the number of reduce tasks, The output gets created in multiple shards. 
         But then there is the overhead of processing every shard.
         Consecutive jobs get slower becasue for the same amount of data, multiple input shards have to be processed.
         Hence time required increases for every further job.
         
Q2) I compared the performance for my map reduce on RCV1.small dataset with and without using Combiner. Since I tested in pseudo distributed mode, I have got the same result. 10 sec for both for the first file NBTrain. Combiner is mainly to reduce network congestion by doing reducer's work at the local level. Hence I think in an actual distributed mode, using a combiner may save time.
         
