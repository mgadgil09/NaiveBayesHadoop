import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import java.io.FileReader;

public class NBClassify {

  public static class inputMapper
       extends Mapper<Object, Text, Text, Text>{
       public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
      StringTokenizer token = new StringTokenizer(value.toString(),"\n");
      while(token.hasMoreTokens()){
        String s = token.nextToken().toString();
         //System.out.println("****"+s+"***");
        if(s.startsWith("*c*")){
           String[] arr = s.split("\\s+");
          //System.out.println("****"+s+"***");
          System.out.println(arr[2] + "blahhhh  " +arr[3]);
           context.write(new Text("*c*"),new Text(arr[2]+","+arr[3]));
        
             }
       }
     }
    }
    
    public static class inputReducer
       extends Reducer<Text,Text,Text,Text> {
       
       public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
   	
   	double pr=0;
   	String counters="";
   	for(Text t:values){
   	
   	counters += t.toString()+":";
   	//String[] arr = t.toString().split(",");
   	//context.write(new Text(arr[0]),new Text(arr[1]));
   	
   	//System.out.println("**********"+counters);
   	}
   	context.write(new Text("counters"),new Text(counters));
      }
    }
  
  public static class ClassifyMapper
       extends Mapper<Object, Text, Text, Text>{

   private final HashMap<String,Double> counterMap = new HashMap<String,Double>();
   @Override
   protected void setup(Context context)throws IOException,InterruptedException{
	Configuration conf = context.getConfiguration();
	//URI[] cacheFile = Job.getInstance(conf).getCacheFiles();	
	//FSDataInputStream in = FileSystem.get(conf).open(new Path(cacheFile[0].getPath()));
	FileSystem fs = FileSystem.get(conf);
	URI[] cacheFile = DistributedCache.getCacheFiles(conf);
	int count = 0;
	for(URI filePath: cacheFile){
		count++;
		System.out.println("FIle is -----"+count);
		Path getPath = new Path(filePath.getPath());
	
	
	
		//BufferedReader joinReader = new BufferedReader(new FileReader(new Path(file.getPath()).getName().toString()));
		BufferedReader joinReader = new BufferedReader(new InputStreamReader(fs.open(getPath)));
		String line;
			//System.out.println("HEEERERERE111111111");
	       while ((line = joinReader.readLine()) != null){
	       				System.out.println("************"+line);
					String[] s = line.toString().split("\t");
					for(String part: s[1].split(":")){
						if (part.equals("") ) continue;
						String[] keyvalue = part.split(",");
						counterMap.put(keyvalue[0], Double.parseDouble(keyvalue[1]) );
					}
					/*for(int i=0;i<s.length;i++)
					{
						if(!s[i].startsWith("w"))
						{
							String[] token = s[i].split("\\s+");
							counterMap.put(token[0],Double.parseDouble(token[1]));
						}
					}*/

				} 
		joinReader.close();
	}
//	System.out.println("HEEERERERE222222");
//	System.out.println(counterMap.size());    	
    }
   
   
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
     HashMap<String,Double> priorMap = new HashMap<String,Double>();
     StringTokenizer token = new StringTokenizer(value.toString(),"\n");
      ArrayList<String> labels = new ArrayList<String>(Arrays.asList("CCAT", "ECAT", "MCAT" , "GCAT"));
      //calculate priors
      
      int documents = counterMap.get("docTotal").intValue();
      int vocab = counterMap.get("vocab").intValue();
      for(String l : labels){
          if(counterMap.containsKey("doc"+l)){
          priorMap.put(l,(counterMap.get("doc"+l)/(double)documents));
                  }
          else{
      	    priorMap.put(l,0.0);
          }    
      }
      while(token.hasMoreTokens())
	     {
		 String tokenLine = token.nextToken().toString();
		 if(!tokenLine.startsWith("*c*")){
		 double pWordLabel,logProb ;
		 HashMap<String,String> modelMap = new HashMap<String,String>();
		 ArrayList<String> testList = new ArrayList<String>();
		 
		 if(tokenLine.contains("*Test*")){
		 String tokens[] = tokenLine.split("\\s+");
		 String[] data = tokens[1].split("\\|");
		 int wordPerLabelCount=0;
		 double allWordsInLabel=0;
		 for(int i=0;i<data.length;i++){
		 	if(data[i].contains("*Model*")){
		 	    String[] modelData = data[i].substring(7,data[i].length()).split(",");
		 	    modelMap.put(modelData[0],modelData[1]);
		 	    
		 	    }
		 	else{
		             testList.add(data[i].substring(6,data[i].length()));
		 	    }
		       }//for
		       
		 
		    for(String label : labels){
		    if(!modelMap.containsKey(label)){
		        wordPerLabelCount = 0;
		       }
		       else{
		       wordPerLabelCount = Integer.parseInt(modelMap.get(label));
		       }
		       if(!counterMap.containsKey(label)){
		       allWordsInLabel = 0;
		       }
		       else{
		        allWordsInLabel = counterMap.get(label);
		        }
		      // System.out.println("counterMap.get(label) :" + counterMap.get(label) );
		    pWordLabel = (double)(wordPerLabelCount+1)/(allWordsInLabel + (double)(vocab));
		    for(String t : testList){
		       String[] testData = t.split("count");
		       String[] docNo = testData[0].split("_");
		       int testWordFrequency = Integer.parseInt(testData[1]);
		        logProb = testWordFrequency * Math.log(pWordLabel);
		       	String valueToReducer = Double.toString(logProb);
		       	double priorVal = Math.log(priorMap.get(label));
		       	
		       	context.write(new Text(label+":"+docNo[1]+"_"+Double.toString(priorVal)),new Text(valueToReducer));
		          }
		  
		  
		      }
		       
	          }//if
	       }//if
	     }//while
   }
  }

  public static class ClassifyReducer
       extends Reducer<Text,Text,Text,Text> {
       
       public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
   	String p="";
   	double pr=0;
   	//double priorVal=0;
   
      for(Text t:values){
     	p = t.toString().trim();
       pr += Double.parseDouble(p);
      }
      String key1 = key.toString();
    //  System.out.println("************"+key1);
      String[] prior = key1.split("_");
      
      pr += Double.parseDouble(prior[1]);
     
       context.write(new Text(prior[0]),new Text(Double.toString(pr)));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Configuration conf1 = new Configuration();
    Job job = new Job(conf, "test");
    job.setJarByClass(NBClassify.class);
    job.setMapperClass(inputMapper.class);
    //job.setCombinerClass(inputReducer.class);
    job.setReducerClass(inputReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//    System.out.println("*************************"+conf.get("fs.defaultFS")+"/"+args[1]+"/part-r-00000");
    job.waitForCompletion(true);
   //conf.get("fs.default.name")+"/"+
    //job.addCacheFile(new Path(args[1]+"/part-r-00000").toUri());
    //job.waitForCompletion(true);
    
       Path cacheFile = new Path(args[1]);
   	FileSystem fs = cacheFile.getFileSystem(conf1);
     FileStatus[] list = fs.listStatus(new Path(args[1]+"/"));
     
  

   //list = fs.globStatus(new Path(conf.get("fs.defaultFS")+"/"+args[1]+"/"));
   
   for(FileStatus f : list){ 

    DistributedCache.addCacheFile(f.getPath().toUri(),conf1);
   System.out.println("Checkpoint!");
   }
    
     Job job1 = Job.getInstance(conf1, "test");
    job1.setJarByClass(NBClassify.class);
    job1.setMapperClass(ClassifyMapper.class);
    //job1.setCombinerClass(ClassifyReducer.class);
    job1.setReducerClass(ClassifyReducer.class);
    job1.setNumReduceTasks(10);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    job1.waitForCompletion(true);
  }
}

