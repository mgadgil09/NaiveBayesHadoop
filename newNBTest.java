import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;

public class newNBTest {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private static IntWritable wordsPerLabelCount;
    private Text wordVocab = new Text();
    private Text label = new Text();
    private Text justLabel = new Text();
    private Text wordFreqPerLabel = new Text();
   
    int count=0;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
      while (itr.hasMoreTokens()) {
      //String[] arr = itr.nextToken().toString().split("\\t");
      String[] arr1 = itr.nextToken().split("_");
      String[] arr2 = arr1[1].split("\\s+");
      arr1[1] = arr2[0].substring(0,4);
      arr2[0] = arr2[0].substring(4);
      justLabel.set(arr1[1]+"label");
      label.set(arr1[1]);
      context.write(new Text(arr1[0]+"_"+arr1[1]),new IntWritable(1));	     
      //System.out.println(arr1[0]+"blahhh "+arr1[1]+"blahhh "+arr2[0]+"blahhhh"+arr2[1]);
      wordsPerLabelCount = new IntWritable(Integer.parseInt(arr2[1].trim()));
      context.write(label,wordsPerLabelCount);
      wordFreqPerLabel.set("w"+arr2[0]+arr1[1]);
      context.write(wordFreqPerLabel,wordsPerLabelCount);
      wordVocab.set("word"+arr2[0]);
      context.write(wordVocab,wordsPerLabelCount);
    //  context.write(new Text(arr1[1]),wordsPerLabelCount);
      }
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
 	 //private java.util.Set<String> set= new java.util.HashSet<String>();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
     
     
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
      
    }
  }

 public static class TokenizerMapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private static IntWritable wordsPerLabelCount;
    private Text wordVocab = new Text();
    private Text label = new Text();
    private Text justLabel = new Text();
    private Text wordFreqPerLabel = new Text();
   
    int count=0;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
      while (itr.hasMoreTokens()) {
      String temp = itr.nextToken().toString();
       String[] arr = temp.split("\\s+");
      if(arr[0].startsWith("word")){
     // String[] arr1 = arr[0].split("\\*");
      context.write(new Text("vocab"),new IntWritable(1));
     }
     else if(arr[0].matches("[a-zA-Z](CAT)")){
      context.write(new Text("labels"),new IntWritable(1));
      context.write(new Text(arr[0]), new IntWritable(Integer.parseInt(arr[1])));
     }
     else if(arr[0].contains("_")){
     String[] doc = arr[0].split("_");
     context.write(new Text("doc"+doc[1]),new IntWritable(1));
     context.write(new Text("docTotal"),new IntWritable(1));
     }
     else{
      context.write(new Text(arr[0]), new IntWritable(Integer.parseInt(arr[1])));
     }
      }
      
    }
  }

  public static class IntSumReducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
 	 //private java.util.Set<String> set= new java.util.HashSet<String>();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
     
     
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
      
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
   
    Job job = Job.getInstance(conf, "NBTest1");
    
    job.setJarByClass(newNBTest.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(10);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
   
    Job job1 = new Job(new Configuration());
     Configuration conf1 = job1.getConfiguration();
     job1.setJobName("Use Cache");
   // System.out.println("fs.default.name : - " + conf1.get("fs.default.name"));
    
    job1.setJarByClass(newNBTest.class);
    job1.setMapperClass(TokenizerMapper1.class);
    //job1.setCombinerClass(IntSumReducer1.class);
    job1.setReducerClass(IntSumReducer1.class);
    job1.setNumReduceTasks(10);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
   // String path = args[1]+"part*";
     FileInputFormat.addInputPath(job1, new Path(args[1]+"/"));
    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    //DistributedCache.createSymlink(co);
    job1.waitForCompletion(true); 
    
    //FileSystem fs = FileSystem.get(conf1);
    //Path pathPattern = new Path(conf1.get("fs.default.name")+"/"+args[2]+"/part-r-00000"); 
    //FileStatus [] shards = fs.globStatus(pathPattern);
    //for (FileStatus shard : shards) {
    //DistributedCache.addCacheFile(shard.getPath().toUri(), conf);
    
  }
}

