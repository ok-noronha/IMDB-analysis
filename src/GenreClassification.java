import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class GenreClassification {

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text();
    int count = 0;

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      // Split input line into movie metadata
      String[] fields = value.toString().split("\t");
      if (fields.length==9) {
    	  String[] genres = fields[8].split(",");
    	  String title = fields[3]; 
    	  String id = fields[0]; 
    	  String type = fields[1];
    	  if (id.equals("tconst")){
    		  return;
    	  }
    	  if (!type.equals("movie")){
    		  return;
    	  }
    	  word.set(id+"@@@"+title);
          for (String genre : genres){
        	  if(genre.equals("/N")) {
    			  continue;
    		  }
        	  if(genre.equals(context.getConfiguration().get("genre")) && count<10){
        	  context.write(new Text(genre), word);
        	  count++;
        	  }
          }
      }
    }
  }

  public static class ConcatReducer extends Reducer<Text, Text, Text, Text> {
	  
	  int count = 0;
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
    	  if (count>=10){
    		  break;
    	  }
        String[] fields = value.toString().split("@@@");
        context.write(new Text(fields[0]), new Text(fields[1]));
        count++;
      }
    }
  }

  public static void run() throws Exception {
    //Configuration conf = new Configuration();
    //Job job = Job.getInstance(conf, "GenreClassification");
    Scanner sc = new Scanner(System.in);
	  
	  Job job = new Job();
    System.out.println("\nEnter Genre :");
    job.getConfiguration().set("genre", sc.nextLine());
    FileSystem fs = FileSystem.get(job.getConfiguration());
    job.setJarByClass(GenreClassification.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(ConcatReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    if (fs.exists(new Path("/recommends"))) {
    	fs.delete(new Path("/recommends"),true);
    }
    FileInputFormat.addInputPath(job, new Path("/genre"));
    FileOutputFormat.setOutputPath(job, new Path("/recommends"));
    job.waitForCompletion(true);
  }
}
