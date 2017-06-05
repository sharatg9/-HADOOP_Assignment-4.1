package task1;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException , InterruptedException{
		
		String line=value.toString().replace("|", ",");
		String[] val=line.split(",");                                  
		int flag=0;
		if(val[0].equalsIgnoreCase("NA") || val[1].equalsIgnoreCase("NA")){                     
			flag=1;                                                           //set the flag equal to 1 if NA is found in 1st or 2nd column  
		}
		
		if(flag!=1){
			context.write(NullWritable.get(), value);                        // Nullwritable provide no output hence no key for
		}
		
		
	}
	
	
}
