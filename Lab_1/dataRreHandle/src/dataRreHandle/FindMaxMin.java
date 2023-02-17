package dataRreHandle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindMaxMin {
	
	static String INPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Filter2/part-r-00000"; 
	static String OUTPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/RatingMaxMin";
	
	public static final class FindMaxMinMapper extends Mapper<Object, Text, DoubleWritable, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			String line = value.toString();
	        String[] values = line.split("\\|");
	        String rating = values[6];
	        
	        if(!rating.equals("?")) {
	        	DoubleWritable rate = new DoubleWritable();
	        	rate.set(Double.valueOf(rating));
	        	context.write(rate, new Text(""));
	        }
	        
		}
		
	}
	
	public static final class FindMaxMinReducer extends Reducer<DoubleWritable, Text, DoubleWritable, DoubleWritable> {
		
		public static double max = 0;
		public static double min = 1000;
		
	    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        
	    	if(key.get() > max) {
	    		max = key.get();
	    	}
	    	if(key.get() < min) {
	    		min = key.get();
	    	}
	    }
	    
	    @Override
	    protected void cleanup(Reducer<DoubleWritable, Text, DoubleWritable, DoubleWritable>.Context context)
	    		throws IOException, InterruptedException {
	    	context.write(new DoubleWritable(max), new DoubleWritable(min));
	    }
	}
	
	public static void main(String[] arg) throws Exception{
		
		Path outputpath=new Path(OUTPUT_PATH);
		Path inputpath=new Path(INPUT_PATH);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "find_max_min");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(FindMaxMin.class);
		job.setMapperClass(FindMaxMinMapper.class);
		job.setReducerClass(FindMaxMinReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
	
}
