import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FacebookMutualFriends {

    public static class MapFacebookMutualFriends
            extends Mapper<LongWritable, Text, Text, Text> {
    	
    	// variable word is storing the pair of facebook urls
    	private Text word = new Text();
    	
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		
    		// for each of the line in the dataset it is loaded as line
    		String[] line = value.toString().split("\t");
    		// filtering lines with size of 2 because first word should tell us the facebook username 
    		// and the second word should show his/her friends
    		if(line.length == 2){
    			
    			// first word is the facebook user
    			String facebookUser = line[0];
    			// split each of its friends by comma and store them into a list
    			List<String> facebookUserFriends = Arrays.asList(line[1].split(","));		
    			// for each of the friend from the stored list
    			for(String friend: facebookUserFriends) {
  	
    				// changing facebook id's to integer to compare
    				int facebookUserIntVal = Integer.parseInt(facebookUser);
    				int friendIntVal = Integer.parseInt(friend);
    				// making the map for two friends in ascending order
    				if(facebookUserIntVal < friendIntVal) {
    					word.set(facebookUserIntVal + "," + friendIntVal);
    				} else {
    					word.set(friendIntVal + "," + facebookUserIntVal);
    				}	
    				// creating a map of two facebook users and whom their commmon friends
    				context.write(word, new Text(line[1]));
    			}
    		}
    	}
    }

    public static class ReducerFacebookMutualFriends
            extends Reducer<Text, Text, Text, Text> {

    	// to store the final reduced key value pair
    	private Text result = new Text();
    	
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
        	// creating a new hash map and string builder. string builder in java represents a mutable sequence of charecters
        	HashMap<String, Integer> map = new HashMap<String, Integer>();
        	StringBuilder stringBuilder = new StringBuilder();
        	
        	// for each of the friend in the values from key value pair which we got from the mapper
        	// reduce or group all the key value pairs by the key. For example (A, B)->C and (A, B)->D into (A,B)->(C,D)
        	for (Text friends : values) {
        		List<String> temp = Arrays.asList(friends.toString().split(","));
        		for(String friend: temp) {
        			if(map.containsKey(friend)) {
        				stringBuilder.append(friend + ',');
        			} else {
        				map.put(friend, 1);
        			}
        		}
        	}
        	        	
        	if(stringBuilder.lastIndexOf(",") > -1) {
        		stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(","));
        	}
        	
        	// writing the reduced key value pair as the results
        	result.set(new Text(stringBuilder.toString()));
        	context.write(key, result);
        	
        }
    }

    public static void main(String[] args) throws Exception {
    	
    	// number of arguments should be exactly 2
    	if(args.length != 2){
    		System.err.println("Ivalid Arguments!!");
    	}
    	
    	// configuration setup
        Configuration conf = new Configuration();
        
        // set job instance
        Job job = Job.getInstance(conf, "MutualFriends");
        // class name
        job.setJarByClass(FacebookMutualFriends.class);
        
        // what is this?
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // set mapper class and reducer class
        job.setMapperClass(MapFacebookMutualFriends.class);
        job.setReducerClass(ReducerFacebookMutualFriends.class);
        
        // set input format and output format
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        // first argument will be the input path and second will be output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}