package task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	
	public static void main(String[] args) throws Exception {
		
		// Create a new Job
		Configuration conf = new Configuration();
		
        // Create job
		Job job = new Job (conf ,"task1");
		job.setJarByClass(Driver.class);
		
		// Setup MapReduce job
		job.setMapperClass(Map.class);
		
		// Specify key / value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// Set reduce task to zero
		job.setNumReduceTasks(0); 
		
		// Input
		FileInputFormat.addInputPath(job, new Path(args[0])) ;
		 
		// Output
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
	    // Execute job	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}
