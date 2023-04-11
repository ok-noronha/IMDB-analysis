import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class PopularMovies {

  public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

	  public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
		  try {
			  // Split input line into rating data
		      String[] fields = value.toString().split("\t");
		      if (fields[1].equals("averageRating")){
		    	  return;
		      }
		      String movieId = fields[0];
			  Double rat = Double.parseDouble(fields[1])*Double.parseDouble(fields[2]);
			  context.write(new Text(fields[0]),new Text("rat!:!"+rat.toString()));
			  
		  }
		  catch(Exception e) {
			  return;			  
		  }
     
	  }
  }
  public static class RatingiMapper extends Mapper<LongWritable, Text, Text, Text> {

	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
	      try{
	    	  String[] fields = value.toString().split("\t");
		      if (fields[3].equals("Original Title")){
	        	  return;
	          }
		      context.write(new Text(fields[0]),new Text("nam!:!"+fields[3]));
		    
	      }
	      catch (Exception e){
	    	  return;	    	  
	      }
	      }
	  }

  public static class RatingReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	  
	  private TreeMap<Double, String> movieRatings = new TreeMap<Double, String>();
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      Double ratings = -1.0;
      String k ="";
      boolean rat = false, tit = false;
      // Compute sum and count of ratings for each movie title
      for (Text v :values){
    	  String[] fs = v.toString().split("!:!");
    	  if (fs[0].equals("rat")){
    		  ratings = Double.parseDouble(fs[1]);
    		  rat=true;
    	  }
    	  else {
    		  k = fs[1].toString() ;  		
    		  tit=true;
    	  }
      }
      if (rat && tit){
     	 movieRatings.put(ratings, key.toString()+"\t"+k+"\t"+ratings.toString());
     	 //context.write(new Text(k), new DoubleWritable(ratings));
      }
      if (rat && !tit){
     	 movieRatings.put(ratings, key.toString()+"\t"+"unknown"+"\t"+ratings.toString());
     	 //context.write(new Text(k), new DoubleWritable(ratings));
      }
      try{
         
      }
      catch (Exception e) {
    	  return;
      }
      if (movieRatings.size() > 20) {
          movieRatings.remove(movieRatings.firstKey());
        }
    }

    protected void cleanup(Context context)throws IOException, InterruptedException{
    	for (String item: movieRatings.descendingMap().values()){
    		context.write(new Text(item), new DoubleWritable());
    	}
    }
  }
  

  public static void run() throws Exception {
    Job job = new Job();
    FileSystem fs = FileSystem.get(job.getConfiguration());
    job.setJarByClass(PopularMovies.class);
    //job.setMapperClass(RatingMapper.class);
    job.setReducerClass(RatingReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    if (fs.exists(new Path("/popular"))) {
    	fs.delete(new Path("/popular"),true);
    }
    MultipleInputs.addInputPath(job, new Path("/ratings"), TextInputFormat.class, RatingMapper.class);
    MultipleInputs.addInputPath(job, new Path("/genre"), TextInputFormat.class, RatingiMapper.class);
    FileOutputFormat.setOutputPath(job, new Path("/popular"));
    job.waitForCompletion(true);
  }
}
