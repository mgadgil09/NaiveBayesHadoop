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

public class NBTestFinal {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    int docCount=0;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
	Pattern p = Pattern.compile("[a-zA-Z](CAT)");
      while (itr.hasMoreTokens()) {
	 docCount++;
	String[] arr = itr.nextToken().toString().split("\\s+");
	Matcher labelMatch = p.matcher(arr[0]);
	List<String> labelArray = new ArrayList<String>();
	while(labelMatch.find())
	{
		labelArray.add(labelMatch.group());
	}	
	//String[] words = arr[1].split("\\s+");	
	for(int x=0;x< labelArray.size();x++)
	{
	  for(int i=1;i<arr.length;i++)
	   {
	      arr[i] = arr[i].replaceAll("\\W", "");
	      arr[i] = arr[i].replaceAll("_", "");
	      if (arr[i].length() > 0){
		String temp = arr[i]+labelArray.get(x)+"_"+docCount;         
        	word.set(temp);
        	context.write(word, new IntWritable(1));
	       }
	   }
	}
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

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
    Job job = Job.getInstance(conf, "test");
    job.setJarByClass(NBTestFinal.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(10);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

