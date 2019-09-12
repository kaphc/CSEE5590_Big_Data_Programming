import java.io.IOException;
import java.util.HashMap;
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

public class MatrixMultiply {

	// mapper class
    public static class MapMatrixElements
            extends Mapper<LongWritable, Text, Text, Text> {
    	
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		
    		// each line is converted to string
    		String line = value.toString();
    		String[] indicesWithValue = line.split(",");
    		
    		// get configuration details
    		Configuration conf = context.getConfiguration();
    		// need only m and p
    		int m = Integer.parseInt(conf.get("m"));
    		int p = Integer.parseInt(conf.get("p"));
    		
    		// initialize output key and output value
    		Text outputKey = new Text();
    		Text outputValue = new Text();
    		
    		// check for matrix M
    		if(indicesWithValue[0].equals("M")) {
    			for(int x = 0; x<p; x++) {
    				
    				// key will be 
    				outputKey.set(indicesWithValue[1] + "," + x);
    				
    				// set value as such from the file
    				outputValue.set(indicesWithValue[0] + "," + indicesWithValue[2] + "," + indicesWithValue[3]);
    				
    				// write output key and value to context 
    				context.write(outputKey, outputValue);
    			}
    		} else {
    			for(int y = 0; y<m; y++) {
    				outputKey.set(indicesWithValue[1] + "," + y);
    				outputValue.set(indicesWithValue[0] + "," + indicesWithValue[2] + "," + indicesWithValue[3]);
    				context.write(outputKey, outputValue);
    			}
    		}
    		    		
    	}
        
    }

    public static class ReduceMatrixElements
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
        	String[] value;
        	
        	// hash map for matrix A
        	HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
        	HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
        	
        	for(Text val : values){
        		value = val.toString().split(",");
        		if(value[0].equals("M")) {
        			hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
        		} else {
        			hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
        		}
        	}
        	
        	int n = Integer.parseInt(context.getConfiguration().get("n"));
        	float result = 0.0f;
        	float m_ij;
        	float n_jk;
        	for(int j = 0; j<n; j++) {
        		m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
        		n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
        		result += m_ij * n_jk;
        	}
        	context.write(null, new Text(key.toString() + "," + Float.toString(result)));
        }
    }

    public static void main(String[] args) throws Exception {
    	
    	// number of arguments should be exactly 2
    	if(args.length != 2){
    		System.err.println("Ivalid Arguments!!");
    	}
    	
    	// configuration setup
        Configuration conf = new Configuration();
        // matrix A with 2x3 and matrix B with 3x2
        conf.set("m", "2");
        conf.set("n", "3");
        conf.set("p", "2");
        
        // set job instance
        Job job = Job.getInstance(conf, "MatrixMultiply");
        // class name
        job.setJarByClass(MatrixMultiply.class);
        
        // what is this?
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // set mapper class and reducer class
        job.setMapperClass(MapMatrixElements.class);
        job.setReducerClass(ReduceMatrixElements.class);
        
        // set input format and output format
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        // first argument will be the input path and second will be output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}