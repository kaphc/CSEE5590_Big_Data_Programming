/*
Group Name:
Member:  name				   student ID
	- Jayden Tran 				16213471
	- Kavin Kumar Arumugam 		16262979
	- Alper Erel 				16254091
	-
Date: 10/06/2019
Lab assignment 1
MapReduce for Youtube dataset.

Dataset: https://umkc.box.com/s/69u5uxkf8v703cqco7tik453izhfg2gu

Question 2: Use Case:
Problem Statement 1:
	FIND OUT WHAT ARE THE TOP 5 CATEGORIES WITH MAXIMUM NUMBER OF VIDEOS UPLOADED.

Column 1: video id of 11 characters.
Column 2: uploader of the video
Column 3: interval between the day of establishment of Youtube and the date of uploading of the video.
Column 4: Category of the video.
Column 5: Length of the video.
Column 6: Number of views for the video.
Column 7: Rating on the video.
Column 8: Number of ratings given for the video
Column 9: Number of comments done on the videos.
Column 10: Related video ids with the uploaded video.

Algorithm:

	create static class inputMap
			get file input as text then store values in this class
			
	create reduceGroup to read and calculate all categories appear in input file
		create a for loop to read each line from input file
			store calculate result in sum variable
				then pass them to this class
				
	Create a main function for output
		recall inputMap and reduceGroup
		
		get values from inputMap and reduceGroup
		
		then set the output format 
		
		then set the output file
	


*/



//Import every possible library
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	//Create top 5 class
   public class top5cata {
	  //create extend mapper for input
    public static class inputMap extends Mapper<LongWritable, Text, Text,
IntWritable> {
    	//declare cate as text type
       private Text cate = new Text();
       //declare private static IntWritable with constand "one"
       private final static IntWritable one = new IntWritable(1);
       
       //Overriding the mapper and get every line from input text
       public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
    	   
    	   //hold and store every string in line
           String line = value.toString();
           //declare string array and split each line by \t
           String str[]=line.split("\t");
           
           /*Create a loop by condition, this loop will stop when the string array (str[])
           greater than 5
           so it can avoid array out of number error*/
         if(str.length > 5){
        	 	//Categories count from first column to column 4,
        	 	//That mean index 3 in array
        	 	//hold the text in cate.set
                cate.set(str[3]);
          }
         //write key and value into context
      context.write(cate, one);
      }

    }
    
    //Create the reduceGroup for final output of MapReduce program
    //take output as the same mapper class
    public static class reduceGroup extends Reducer<Text, IntWritable,
Text, IntWritable> {
    	
    	//override the reducegroup method for every keys and values
       public void reduce(Text keys, Iterable<IntWritable> vals,
Context cont)throws IOException, InterruptedException 
			//throws out 2 exceptions to avoid errors
       		{
    	   
    	   //declare variable sum to store all values for each key
           int sum = 0;
           //create for loop to run values inside iterable values, 
           //and sort after the mapper phase
           for (IntWritable val : vals) {
        	   	//get all categories count. 
        	   //sum them together by values        	   
               sum += val.get();
           }
           
           //store each categories key and sum result to context
           cont.write(keys, new IntWritable(sum));
       }
    }
    
    //this main function to recall inputMap and reduceGroup classes
    //then create the output format for final result
    public static void main(String[] cate_agru) throws Exception {
    	
    	//declare configuration method as config
       Configuration config = new Configuration();
       		
       	   //disable compilation for deprecated code
           @SuppressWarnings("deprecation")
           		//declare job method as job to carry config for categories values
                Job job = new Job(config, "categories");
           
           //Create jar file top 5 class
           job.setJarByClass(top5cata.class);
           
           
           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(IntWritable.class);
      
       //set input and output format class
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       
       //set output key and value class
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
       
       //set mapper and reduce classes
       job.setMapperClass(inputMap.class);
       job.setReducerClass(reduceGroup.class);

       //create the condition if missing arguments from passing values
       if(cate_agru.length < 2){
    	   System.out.println(cate_agru.length);
    	   System.err.println("not enough arguments");
       }
       
       //add inputPath and set output path
       FileInputFormat.addInputPath(job, new Path(cate_agru[0]));
       FileOutputFormat.setOutputPath(job, new Path(cate_agru[1]));
      
       //declare new Path for output
       Path out=new Path(cate_agru[1]);
       
       //file out and wait for the debug finish
       out.getFileSystem(config).delete(out);
       job.waitForCompletion(true);
    }

  }
