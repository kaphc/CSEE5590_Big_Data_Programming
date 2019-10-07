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
Problem Statement 2:
	FIND THE TOP 10 RATED VIDEOS ON YOUTUBE.

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
	//this algorithm similar to the problem 1, only change the number of index 
	 * from 3 to 7. It's the rate number at.

inputfile   ===>create static class inputMap
				|		get file input as text then store values in this class
				|
				|
				V		
inputfile   ===>create reduceGroup to read and calculate all categories appear in input file
				|	create a for loop to read each line from input file
				|		store calculate result in sum variable
				|			then pass them to this class
				V			
				Create a main function for output
				|	recall inputMap and reduceGroup
				|	
				|	get values from inputMap and reduceGroup
				|	
				|	then set the output format 
				|	
				|	then set the output file
				|		
				V
				File Out

*/





import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.*;


	//create a class videoRating
   public class videoRating {
	
	//create inputMap and extend it by mapper
    public static class inputMap extends Mapper<LongWritable, Text, Text,
FloatWritable> {
    	
    	//declare private variables
       private Text videoName = new Text();
       private  FloatWritable rate = new FloatWritable();
       
       //create overideMap, this run one time for every line
       public void overideMap(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
    	   
    	   //declare inputline as string type, then get read value from each line in file
           String inputline = value.toString();
           
           //declear string array as str[]
           String str[]=inputline.split("\t"); //split the value by \t
           
           //create the loop, the index 7 in array is rate column
          if(str.length > 7){
        	  	//get video name from array index 0
                videoName.set(str[0]);
                //this regular expression
                //only contain float values in case of rate number round up to 5
                
                //declear f as float type
                float f=Float.parseFloat(str[6]); //convert string to float
                //stored f in rate
                rate.set(f);
                
          }
         
      //store videoname and rate into context
      context.write(videoName, rate);
      }

    }
    
    
  //Create the reduceGroup for final output of MapReduce program
    //take output as the same mapper class
    public static class reduceGroup extends Reducer<Text, FloatWritable,
Text, FloatWritable> {
    	
    	//override the reducegroup method for every keys and values
       public void reduce(Text key, Iterable<FloatWritable> vals,
Context cont)throws IOException, InterruptedException {
    	 //throws out 2 exceptions to avoid errors
    	   
    	   //declare sum for hold calculate result
    	   float sum = 0;
           
    	   //declare count as increment to count values for the key
    	   int count=0;
    	   
    	   //create for loop to read every line by iterable values
           for (FloatWritable val : vals) {//float type
                   count+=1;  //counts number of values are there for that key
               sum += val.get(); //sum up rate value 
           }
           sum=sum/count;   //takes the average of the sum
           
           //store in context cont
           cont.write(key, new FloatWritable(sum));
       }
    }
    
    //this main function to recall inputMap and reduceGroup classes
    //then create the output format for final result    
    public static void main(String[] argument) throws Exception {
    	//declare configuration method as config
       Configuration config = new Configuration();
       	
     //disable compilation for deprecated code
           @SuppressWarnings("deprecation")
         //declare job method as job to carry config for categories values
           		Job job = new Job(config, "videorating");
           
           //Create jar file for videoRating class
           job.setJarByClass(videoRating.class);
           
           
           //set map output key and value
           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(FloatWritable.class);
      
       //set output key and value
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(FloatWritable.class);
       
       //recall inputMap and reduceGroup and set them as
       //mapper and reducer class
       job.setMapperClass(inputMap.class);
       job.setReducerClass(reduceGroup.class);
       
       
       //set input and output format
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       
       //add input and output format into Path
       FileInputFormat.addInputPath(job, new Path(argument[0]));
       FileOutputFormat.setOutputPath(job, new Path(argument[1]));
       
       //declare new Path as out and store argument
        Path out=new Path(argument[1]);
        //create file out and wait for debug finish
        out.getFileSystem(config).delete(out);
       job.waitForCompletion(true);
    }

  }