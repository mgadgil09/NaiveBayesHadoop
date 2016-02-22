import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
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

public class NBJoin {

  public static class TrainModelMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
     StringTokenizer token = new StringTokenizer(value.toString(),"\n");
     while(token.hasMoreTokens())
     {
	 String modelToken = token.nextToken().toString();
       	 if(modelToken.startsWith("w")){
               String[] wordToken = modelToken.split("\\s+");
               String[] newTokens = new String[3];
               newTokens[0] = wordToken[0].substring(1, wordToken[0].length()-4);
               newTokens[1] = wordToken[0].substring(wordToken[0].length()-4,wordToken[0].length());
               newTokens[2] = wordToken[1];
               context.write(new Text(newTokens[0]),new Text("*Model*"+newTokens[1]+","+newTokens[2]+"|")); 
     	  }
     	 else{
     	 	context.write(new Text("*c*"+modelToken),value);
     	 } 
     }
   }
  }

public static class TestMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
     StringTokenizer token = new StringTokenizer(value.toString(),"\n");
     while(token.hasMoreTokens())
     {
	 String testToken = token.nextToken().toString();
       	
               String[] wordToken = testToken.split("\\s+");
               String[] wordToken1 = wordToken[0].split("_");
               String[] newTokens = new String[4];
               newTokens[0] = wordToken1[0].substring(0, wordToken1[0].length()-4);
               newTokens[1] = wordToken1[0].substring(wordToken1[0].length()-4,wordToken1[0].length());
               newTokens[2] = wordToken1[1];
               newTokens[3] = wordToken[1];
               context.write(new Text(newTokens[0]),new Text("*Test*"+newTokens[1]+"_"+newTokens[2]+"count"+newTokens[3]+"|")); 
     	  
     }
   }
  }
	
  public static class JoinerReducer
       extends Reducer<Text,Text,Text,Text> {
       
       public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
   	String s="";
   	for(Text t:values){
   
   	s += t.toString();
   	
   	}
      context.write(key,new Text(s));
      }
    }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "test");
    job.setJarByClass(NBJoin.class);
    job.setMapperClass(TrainModelMapper.class);
    job.setCombinerClass(JoinerReducer.class);
    job.setReducerClass(JoinerReducer.class);
    job.setNumReduceTasks(10);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, TrainModelMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,TestMapper.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.waitForCompletion(true);
  }
}

