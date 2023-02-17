package dataRreHandle;

import java.io.IOException;
import java.util.Random;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sample {
	
	static String INPUT_PATH = "hdfs://localhost:9000/input/dataprehandle/data.txt"; 
	static String OUTPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Sample";
	
	public static final class SampleMapper extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			String line = value.toString();
	        String[] values = line.split("\\|");
	        Text career = new Text();
	        career.set(values[10]);
	        context.write(career, value);
		}
		
	}

	public static final class SampleReducer extends Reducer<Text, Text, Text, Text> {
		
		public static final Random random = new Random();
		
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        for (Text value : values) {
	            if (random.nextInt(10000) < 10) {
	                context.write(value, new Text(""));
	            }
	        }
	    }
	}
	
	public static void main(String[] arg) throws Exception{
		
		Path outputpath=new Path(OUTPUT_PATH);
		Path inputpath=new Path(INPUT_PATH);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "sample");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(Sample.class);
		job.setMapperClass(SampleMapper.class);
		job.setReducerClass(SampleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
	
}
